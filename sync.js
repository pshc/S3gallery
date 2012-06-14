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

// Crawling

function findAlbums(dir, callback) {
	console.log("Searching", dir);
	listDirectory(dir, function (err, info) {
		if (err)
			return callback(err);
		if (info.objects.length) {
			console.log("Contains " + info.objects.length + " images");
		}
		async.forEachSeries(info.dirs, function (subdir, callback) {
			var pref = removePrefix(config.AWS.prefix, subdir.Prefix);
			if (pref.match(/thumbs/))
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
		async.filterSeries(album.needThumbs, isDownloaded, function (needDownloads) {
			var n = needDownloads.length;
			if (n)
				console.log('Downloading ' + pluralize(n, 'image') + '.');
			async.forEachSeries(needDownloads, downloadImage, next);
		});
	},
	function (next) {
		var n = album.oldThumbs.length;
		if (n)
			console.log('Deleting ' + pluralize(n, 'old thumbnail') + '.');
		forEachNParallel(4, album.oldThumbs, function (thumb, cb) {
			s3.del(config.AWS.prefix + 'thumbs/' + dir + thumb, cb);
		}, next);
	},
	function (next) {
		var n = album.needThumbs.length;
		if (n)
			console.log('Thumbnailing ' + pluralize(n, 'image') + '.');
		forEachNParallel(3, album.needThumbs, thumbnailAndUploadImage, next);
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
		albumURL = 'http://' + config.AWS.bucket + '/' + config.AWS.prefix + 'thumbs/' + dir;
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
		imgs.push({full: name, thumb: image.meta.thumbName});
		hash.update(name);
		hash.update(image.meta.thumbName);
	});
	album.subdirs.forEach(function (dir) {
		hash.update(dir.path);
	});
	var object = {
		images: imgs,
		dirs: album.subdirs,
	};
	var js = config.AWS.prefix + 'thumbs/' + albumPath + 'index.js';
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
	headers['Cache-Control'] = 'max-age=3600';
	headers[indexHashHeader] = index.hash;
	s3.putBuffer(index.path, buf, 'public-read', headers, callback);
}

// index.html

var jQueryJs = 'jquery-1.7.2.min.js';

function buildHtml(dir) {
	var title = path.basename(dir);
	var html = '<!DOCTYPE html>\n<title>' + htmlEscape(title) + '</title>\n<meta charset=UTF-8>\n'
	html += '<meta name=viewport content="width=device-width; minimum-scale=1.0; maximum-scale=1.0">\n';
	html += '<body><noscript>Javascript required.</noscript></body>\n';
	html += '<script>var config = ' + JSON.stringify(config.visual) + ';</script>\n';
	var level = dir.match(/[^\/]\//g).length;
	var jsPath = new Array(level + 1).join('../');
	var scripts = [jQueryJs, 'gallery.js'];
	scripts.forEach(function (js) {
		if (!js.match(/^https?:\/\//))
			js = jsPath + js;
		html += '<script src="' + encodeURI(js) + '"></script>\n';
	});
	var buf = new Buffer(html, 'UTF-8');
	var s3path = config.AWS.prefix + 'thumbs/' + dir + 'index.html';
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

function scanAlbum(dir, callback) {
	async.parallel({
		images: listDirectory.bind(null, dir),
		thumbs: listDirectory.bind(null, 'thumbs/' + dir),
	}, function (err, listings) {
		if (err)
			return callback(err);
		var thumbs = {};
		var needThumbnails = [];
		var allImages = [];
		// Find photos without up-to-date thumbnails
		listings.thumbs.objects.forEach(function (thumb) {
			var thumbName = path.basename(thumb.Key);
			if (thumbName.match(/_\w{6}\.jpg$/i))
				thumbs[thumbName] = new Date(thumb.LastModified);
		});
		listings.images.objects.forEach(function (image) {
			if (!path.extname(image.Key).match(config.validExtensions))
				return;
			image.meta = imageMeta(image, dir);
			allImages.push(image);
			var thumbDate = thumbs[image.meta.thumbName];
			if (thumbDate) {
				delete thumbs[image.meta.thumbName];
				if (thumbDate > new Date(image.LastModified))
					return;
			}
			needThumbnails.push(image);
		});
		var subdirs = listings.images.dirs.map(function (subdir) {
			return {path: removePrefix(config.AWS.prefix + dir, subdir.Prefix)};
		});
		callback(null, {
			needThumbs: needThumbnails,
			allImages: allImages,
			subdirs: subdirs,
			oldThumbs: Object.keys(thumbs),
		});
	});
}

var thumbnailConfHash;

function imageMeta(image, albumDir) {
	var info = {md5: objectMD5(image)};
	info.ext = path.extname(image.Key);
	info.localPath = path.join(config.scratchDir, info.md5 + info.ext);

	// Generate thumbnail name based on all relevant configuration and
	// image data so that we can easily detect stale thumbnails
	if (!thumbnailConfHash)
		thumbnailConfHash = consistentObjectHash(config.visual.thumbnail);
	var hash = crypto.createHash('md5');
	hash.update(thumbnailConfHash);
	hash.update(info.md5);
	hash = hash.digest('hex').slice(0, 6);
	var prefix = path.basename(image.Key).slice(0, -info.ext.length);
	info.thumbName = prefix + '_' + hash + '.jpg';
	info.thumbRemotePath = config.AWS.prefix + 'thumbs/' + albumDir + info.thumbName;

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

function thumbnailAndUploadImage(image, callback) {
	thumbnailImage(image.meta.localPath, function (err, tmp) {
		if (err)
			return callback(err);
		uploadThumbnail(tmp, image, function (err) {
			fs.unlink(tmp);
			callback(err);
		});
	});
}

function thumbnailImage(filename, callback) {
	var tmp = tempJpegFilename();
	var args = [filename];
	var cfg = config.visual.thumbnail;
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

function uploadThumbnail(localPath, image, callback) {
	var dest = image.meta.thumbRemotePath;
	console.log("Uploading " + dest + "...");
	var headers = reducedHeaders('image/jpeg');
	s3.putFile(dest, localPath, 'public-read', headers, callback);
}

// Support files

var supportFiles = ['gallery.js', 'plain.css', jQueryJs];
var supportMimes = {
	'.js': 'application/javascript',
	'.css': 'text/css',
};

function uploadSupportFiles(callback) {
	async.forEach(supportFiles, function (file, callback) {
		var awsFile = config.AWS.prefix + 'thumbs/' + file;
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
			console.log('Uploading ' + file + '...');
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
		operation(items[thisIndex], function (err) {
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
		setTimeout(processOne, 0);
	}

	function finish() {
		if (pending.length)
			return;
		if (!error && index < items.length)
			return;
		if (done)
			throw new Error("forEachN completed twice?!");
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
