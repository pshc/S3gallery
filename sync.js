var async = require('async'),
    config = require('./config'),
    crypto = require('crypto'),
    events = require('events'),
    fs = require('fs'),
    imagemagick = require('imagemagick'),
    path = require('path'),
    progress = require('progress');

var s3 = require('aws2js').load('s3', config.AWS.key, config.AWS.secret);
s3.setBucket(config.AWS.bucket);

if (!config.browsePath || !config.browsePath.match(/^\w+\/$/))
	throw new Error("Bad browse/ path. Careful!");
var browsePrefix = config.AWS.prefix + config.browsePath;

// Crawling

function findAlbums(dir, callback) {
	console.log("Searching", dir);
	listDirectory(dir, function (err, info) {
		if (err)
			return callback(err);
		var photos = info.objects.filter(isPhoto);
		var n = photos.length;
		if (n)
			console.log('Contains ' + pluralize(n, 'image'));
		async.forEachSeries(info.dirs, function (subdir, callback) {
			var pref = removePrefix(config.AWS.prefix, subdir.Prefix);
			if (pref == config.browsePath)
				return callback(null);
			setTimeout(findAlbums.bind(null, pref, callback), 0);
		}, callback);
	});
}

function updateAlbum(dir, callback) {
	var album, albumURL, changed;
	async.waterfall([
	function (next) {
		scanAlbum(dir, next);
	},
	function (scan, next) {
		album = scan;
		console.log('Album: ' + dir);
		async.filterSeries(album.needResize, isDownloaded, function (needDownloads) {
			var n = needDownloads.length;
			if (n)
				console.log('Downloading ' + pluralize(n, 'image') + '.');
			async.forEachSeries(needDownloads, downloadImage, next);
		});
	},
	function (next) {
		var n = album.oldDerived.length;
		if (n)
			console.log('Deleting ' + pluralize(n, 'old thumbnail') + '.');
		forEachNParallel(4, album.oldDerived, function (thumb, cb) {
			s3.del(browsePrefix + dir + thumb, cb);
		}, next);
	},
	function (next) {
		var n = album.needResize.length;
		if (n)
			console.log('Thumbnailing ' + pluralize(n, 'image') + '.');
		forEachNParallel(3, album.needResize, resizeAndUploadImage, next);
	},
	function (next) {
		var index = buildIndex(album, dir);
		checkIndex(index, function (err, upToDate) {
			if (err)
				next(err);
			else if (upToDate)
				next(null);
			else {
				changed = true;
				console.log('Updating index..');
				uploadIndex(index, function (err) {
					next(err);
				});
			}
		});
	},
	function (next) {
		var html = buildHtml(dir);
		albumURL = 'http://' + config.AWS.bucket + '/' + browsePrefix + dir;
		checkHtml(html, function (err, upToDate) {
			if (err)
				next(err);
			else if (upToDate)
				next(null);
			else {
				changed = true;
				console.log('Updating HTML..');
				uploadHtml(html, function (err) {
					next(err);
				});
			}
		});
	},
	function (next) {
		if (changed)
			console.log('Album updated:', albumURL);
		next(null);
	},
	], callback);
}

// index.js

var indexHashHeader = 'x-amz-meta-index-hash';

function buildIndex(album, albumPath) {
	var version = '1';
	var hash = crypto.createHash('md5').update(version + '\n');
	var imgs = [];
	album.allImages.forEach(function (image) {
		var name = path.basename(image.Key);
		imgs.push({full: name, thumb: image.meta.thumbName, med: image.meta.medName});
		hash.update(name);
		hash.update(image.meta.thumbName);
		hash.update(image.meta.medName);
	});
	var subdirs = album.subdirs;
	if (albumPath)
		subdirs = [{path: '../'}].concat(subdirs);
	subdirs.forEach(function (dir) {
		hash.update(dir.path);
	});
	var object = {
		images: imgs,
		dirs: subdirs,
	};
	var js = browsePrefix + albumPath + 'index.js';
	return {object: object, hash: hash.digest('hex'), path: js, version: version};
}

function checkIndex(index, callback) {
	s3.head(index.path, function (err, headers) {
		if (err)
			return callback(null, false);
		callback(null, headers[indexHashHeader] == index.hash);
	});
}

function uploadIndex(index, callback) {
	var buf = new Buffer(JSON.stringify(index.object), 'UTF-8');
	var headers = reducedHeaders('application/json;charset=UTF-8');
	headers['Cache-Control'] = config.cacheIndexes ? 'public' : 'must-revalidate';
	headers[indexHashHeader] = index.hash;
	s3.putBuffer(index.path, buf, 'public-read', headers, callback);
}

// index.html

var scripts = ['deps/jquery-1.7.2.min.js', 'deps/lightbox/lightbox.js', 'gallery.js'];
var stylesheets = ['plain.css', 'deps/lightbox/lightbox.min.css'];

function buildHtml(dir) {
	var level = dir.match(/[^\/]\//g).length;
	var mediaPath = new Array(level + 1).join('../');
	var title = path.basename(dir);
	var html = '<!DOCTYPE html>\n<title>' + htmlEscape(title) + '</title>\n<meta charset=UTF-8>\n'
	html += '<meta name=viewport content="width=device-width; minimum-scale=1.0; maximum-scale=1.0">\n';
	stylesheets.forEach(function (css) {
		css = mediaPath + removePrefix('deps/', css);
		html += '<link rel="stylesheet" href="' + encodeURI(css) + '">\n';
	});
	html += '<body><noscript>Javascript required.</noscript><section></section></body>\n';
	html += '<script>var config = ' + JSON.stringify(config.visual) + ';</script>\n';
	scripts.forEach(function (js) {
		js = mediaPath + removePrefix('deps/', js);
		html += '<script src="' + encodeURI(js) + '"></script>\n';
	});
	var buf = new Buffer(html, 'UTF-8');
	var s3path = browsePrefix + dir + 'index.html';
	return {buf: buf, path: s3path};
}

function checkHtml(html, callback) {
	s3.head(html.path, function (err, meta) {
		if (err)
			return callback(null, false);
		var md5 = crypto.createHash('md5').update(html.buf).digest('hex');
		return callback(null, md5 == objectMD5(meta));
	});
}

function uploadHtml(html, callback) {
	var headers = reducedHeaders('text/html;charset=UTF-8');
	s3.putBuffer(html.path, html.buf, 'public-read', headers, callback);
}

// Album inspection

var derivedKinds = ['thumb', 'med'];

function scanAlbum(dir, callback) {
	async.parallel({
		images: listDirectory.bind(null, dir),
		thumbs: listDirectory.bind(null, config.browsePath + dir),
	}, function (err, listings) {
		if (err)
			return callback(err);
		var derived = {};
		derivedKinds.forEach(function (kind) {
			derived[kind] = {};
		});
		var needResize = [];
		var allImages = [];
		// Find photos without up-to-date thumbnails
		listings.thumbs.objects.forEach(function (thumb) {
			var thumbName = path.basename(thumb.Key);
			var match = thumbName.match(/_(\w+)_\w{6}\.jpg$/i);
			if (!match)
				return;
			var kindMap = derived[match[1]];
			if (kindMap)
				kindMap[thumbName] = new Date(thumb.LastModified);
		});
		listings.images.objects.forEach(function (image) {
			if (!isPhoto(image))
				return;
			image.meta = imageMeta(image, dir);
			allImages.push(image);
			var resizeNeeded = false;
			derivedKinds.forEach(function (kind) {
				var name = image.meta[kind + 'Name'];
				var kindMap = derived[kind];
				var date = kindMap[name];
				if (date) {
					delete kindMap[name];
					if (date > new Date(image.LastModified))
						return;
				}
				resizeNeeded = true;
				image.meta[kind + 'NeedsUpdate'] = true;
			});
			if (resizeNeeded)
				needResize.push(image);
		});
		var subdirs = listings.images.dirs.map(function (subdir) {
			return {path: removePrefix(config.AWS.prefix + dir, subdir.Prefix)};
		});

		var old = [];
		derivedKinds.forEach(function (kind) {
			old = old.concat(Object.keys(derived[kind]));
		});
		callback(null, {
			needResize: needResize,
			allImages: allImages,
			subdirs: subdirs,
			oldDerived: old,
		});
	});
}

var confHashes = {};

function imageMeta(image, albumDir) {
	var info = {md5: objectMD5(image)};
	info.ext = path.extname(image.Key);
	info.localPath = path.join(config.scratchDir, info.md5 + info.ext);

	derivedKinds.forEach(function (kind) {
		// Generate thumbnail name based on all relevant configuration and
		// image data so that we can easily detect stale thumbnails
		if (!confHashes[kind])
			confHashes[kind] = consistentObjectHash(config.visual[kind]);

		var hash = crypto.createHash('md5');
		hash.update(confHashes[kind]);
		hash.update(info.md5);
		hash = hash.digest('hex').slice(0, 6);
		var prefix = path.basename(image.Key).slice(0, -info.ext.length);
		var name = prefix + '_' + kind + '_' + hash + '.jpg';
		info[kind+'Name'] = name;
		info[kind+'RemotePath'] = browsePrefix + albumDir + name;
	});

	return info;
}

function isDownloaded(image, callback) {
	fs.stat(image.meta.localPath, function (err) {
		callback(!!err);
	});
}

// Album mutation

function downloadImage(image, callback) {
	var tmp = tempJpegFilename();
	console.log("Downloading " + image.Key + "...");
	s3.get(image.Key, 'stream', function (err, resp) {
		if (err)
			return callback(err);
		progressDownload(resp, parseInt(image.Size, 10), tmp, function (err) {
			if (err)
				return callback(err);
			fs.rename(tmp, image.meta.localPath, function (err) {
				if (err)
					return callback(err);
				console.log("Downloaded to: " + image.meta.localPath);
				callback(null);
			});
		});
	});
}

var resizers = {};

function resizeAndUploadImage(image, callback) {
	var ops = [];
	derivedKinds.forEach(function (kind) {
		if (image.meta[kind+'NeedsUpdate'])
			ops.push(_resizeUploadHelper.bind(null, kind, image));
	});
	async.series(ops, callback);
}

function _resizeUploadHelper(kind, image, callback) {
	resizers[kind](image.meta.localPath, function (err, tmp) {
		if (err)
			return callback(err);
		uploadImage(tmp, image.meta[kind+'RemotePath'], function (err) {
			fs.unlink(tmp);
			callback(err);
		});
	});
}

resizers.thumb = function (filename, callback) {
	var tmp = tempJpegFilename();
	var args = [filename];
	var cfg = config.visual.thumb;
	var size = assembleDimensions(cfg.size);
	args.push('-thumbnail', size + '^');
	args.push('-extent', size);
	args.push('-auto-orient');
	args.push('-colorspace', 'sRGB');
	args.push('-strip');
	args.push('-quality', cfg.quality);
	args.push('jpg:' + tmp);
	imagemagick.convert(args, function (err) {
		if (err)
			return callback(err);
		callback(null, tmp);
	});
}

resizers.med = function (filename, callback) {
	var tmp = tempJpegFilename();
	var args = [filename];
	var cfg = config.visual.med;
	args.push('-auto-orient');
	args.push('-gamma', '0.454545');
	args.push('-filter', 'Lagrange');
	args.push('-resize', assembleDimensions(cfg.size) + '>');
	args.push('-gamma', '2.2');
	args.push('-quality', cfg.quality);
	args.push('jpg:' + tmp);
	imagemagick.convert(args, function (err) {
		if (err)
			return callback(err);
		callback(null, tmp);
	});
}

function uploadImage(localPath, dest, callback) {
	console.log("Uploading " + dest + "...");
	var headers = reducedHeaders('image/jpeg');
	s3.putFile(dest, localPath, 'public-read', headers, callback);
}

// Support files

var supportFiles = scripts.concat(stylesheets);
var lightboxImages = ['close.png', 'loading.gif', 'next.png', 'prev.png'];
supportFiles = supportFiles.concat(lightboxImages.map(function (image) {
	return 'deps/lightbox/' + image;
}));
var supportMimes = {
	'.js': 'application/javascript',
	'.css': 'text/css',
	'.png': 'image/png',
	'.gif': 'image/gif',
};

function uploadSupportFiles(callback) {
	async.forEach(supportFiles, function (file, callback) {
		var baseFile = removePrefix('deps/', file);
		var awsFile = config.AWS.prefix + config.browsePath + baseFile;
		s3.head(awsFile, function (err, meta) {
			if (err)
				return upload();
			fs.readFile(file, function (err, buf) {
				if (err)
					return callback(err);
				var md5 = crypto.createHash('md5').update(buf).digest('hex');
				if (md5 != objectMD5(meta))
					upload();
				else
					callback(null);
			});
		});
		function upload() {
			console.log('Uploading ' + baseFile + '...');
			var headers = reducedHeaders(supportMimes[path.extname(file)]);
			s3.putFile(awsFile, file, 'public-read', headers, callback);
		}
	}, callback);
}

// Helpers

function assembleDimensions(dims) {
	return dims.join ? dims.join('x') : dims + '@';
}

function progressDownload(stream, len, dest, callback) {
	var bar = new progress('[:bar] :percent :etas', {total: len, width: 30, incomplete: ' '});
	var file = fs.createWriteStream(dest);
	stream.pipe(file);
	stream.on('data', function (buf) {
		bar.tick(buf.length);
	});
	stream.on('end', function () {
		console.log();
		callback(null);
	});
	stream.on('error', function (err) {
		console.log();
		callback(err);
	});
}

function tempJpegFilename() {
	var rand = Math.ceil(Math.random() * 1e12).toString(36);
	return path.join(config.scratchDir, 'tmp_' + rand + '.jpg');
}

function isPhoto(image) {
	return path.extname(image.Key).match(config.validExtensions);
}

function listDirectory(dir, callback) {
	var lister = createDirectoryLister(dir);
	var objects = [], dirs = [];
	lister.on('objects', function (objs) { objects.push.apply(objects, objs); });
	lister.on('dirs', function (ds) { dirs.push.apply(dirs, ds); });
	lister.once('end', callback.bind(null, null, {objects: objects, dirs: dirs}));
	lister.once('error', callback.bind(null));
}

function createDirectoryLister(dir) {
	var lister = new events.EventEmitter;
	var options = {prefix: config.AWS.prefix + dir, delimiter: '/'};
	_fetchListing(options, lister);
	return lister;
}

function _fetchListing(options, lister) {
	s3.get('/', options, 'xml', function (err, listing) {
		if (err)
			return lister.emit('error', err);
		var items = listing.Contents;
		if (items)
			lister.emit('objects', items instanceof Array ? items : [items]);
		var dirs = listing.CommonPrefixes;
		if (dirs)
			lister.emit('dirs', dirs instanceof Array ? dirs : [dirs]);
		if (listing.IsTruncated != 'true')
			return lister.emit('end');
		options.marker = listing.NextMarker;
		setTimeout(_fetchListing.bind(null, options, lister), 0);
	});
}

function consistentObjectHash(obj) {
	var hash = crypto.createHash('md5');
	_consist.call(hash, obj);
	return hash.digest('hex');
}

function _consist(obj) {
	if (obj instanceof Array)
		obj.forEach(_consist.bind(this));
	else if (typeof obj != 'object')
		this.update(JSON.stringify(obj));
	else {
		var keys = Object.keys(obj);
		keys.sort();
		var self = this;
		keys.forEach(function (key) {
			self.update(key);
			_consist.call(self, obj[key]);
		});
	}
}

function forEachNParallel(n, items, operation, callback) {
	var index = 0, pending = [], error = null, done = false;

	function processOne() {
		if (error || index >= items.length) {
			finish();
			return;
		}
		if (pending.length >= n)
			return;
		var thisIndex = index++;
		pending.push(thisIndex);
		var instantReturn = false;
		operation(items[thisIndex], function (err) {
			instantReturn = true;
			var pos = pending.indexOf(thisIndex);
			if (pos < 0) {
				error = "Callback called twice.";
				finish();
				return;
			}
			pending.splice(pos, 1);
			if (err)
				error = err;
			processOne();
		});
		if (!instantReturn)
			setTimeout(processOne, 0);
	}

	function finish() {
		if (done || pending.length)
			return;
		if (!error && index < items.length)
			return;
		done = true;
		callback(error);
	}

	processOne();
}

function pluralize(n, noun) {
	return n + ' ' + noun + (n==1 ? '' : 's');
}

function removePrefix(prefix, str) {
	return str.slice(0, prefix.length) == prefix ? str.slice(prefix.length) : str;
}

var htmlEscapes = {'<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;'};
function htmlEscape(text) {
	return text.replace(/(<|>|&|")/g, function (c) { return htmlEscapes[c]; });
}

function objectMD5(obj) {
	return (obj.ETag || obj.etag).replace(/"/g, '').toLowerCase();
}

function reducedHeaders(mime) {
	return {
		'Content-Type': mime,
		'x-amz-storage-class': 'REDUCED_REDUNDANCY',
	};
}

// Glue

function setup(callback) {
	var dir = config.scratchDir;
	fs.stat(dir, function (err, stat) {
		if (err)
			return fs.mkdir(dir, cleanUpTemporaries);
		if (!stat.isDirectory())
			return callback('Scratch dir is not a directory');
		cleanUpTemporaries(null);
	});

	function cleanUpTemporaries(err) {
		if (err)
			return callback(null);
		fs.readdir(dir, function (err, files) {
			if (err)
				return callback(null);
			var garbage = [];
			files.forEach(function (file) {
				if (file.match(/^tmp_/))
					garbage.push(path.join(dir, file));
			});
			forEachNParallel(5, garbage, fs.unlink.bind(fs), callback);
		});
	}
}

if (require.main === module) {
	async.series([
		setup,
		uploadSupportFiles,
	], function (err) {
		if (err)
			throw err;
		updateAlbum(config.testAlbum, function (err) {
			if (err)
				throw err;
			console.log("Done.");
		});
	});
}
