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

function updateAlbum(dir, callback) {
	var needThumbs, allImages;
	async.waterfall([
	function (next) {
		scanAlbum(dir, next);
	},
	function (thumbs, images, next) {
		needThumbs = thumbs;
		allImages = images;

		console.log('Album: ' + dir);
		async.filterSeries(needThumbs, isDownloaded, function (needDownloads) {
			var n = needDownloads.length;
			if (n)
				console.log('Downloading ' + pluralize(n, 'image') + '.');
			async.forEachSeries(needDownloads, downloadImage, next);
		});
	},
	function (next) {
		var n = needThumbs.length;
		if (n)
			console.log('Thumbnailing ' + pluralize(n, 'image') + '.');
		async.forEachSeries(needThumbs, thumbnailAndUploadImage, next);
	},
	function (next) {
		var index = buildIndex(allImages, dir);
		checkIndex(index, function (err, upToDate) {
			if (upToDate)
				next(null);
			else {
				var buf = new Buffer(JSON.stringify(index.object), 'UTF-8');
				var headers = {
					'Content-Type': 'application/json;charset=UTF-8',
					'Cache-Control': 'max-age=3600',
					'x-amz-storage-class': 'REDUCED_REDUNDANCY',
				};
				headers[indexHashHeader] = index.hash;
				s3.putBuffer(index.path, buf, 'public-read', headers, function (err) {
					if (err)
						return next(err);
					console.log('Updated album index.');
					next(null);
				});
			}
		});
	},
	], callback);
}

var indexHashHeader = 'x-amz-meta-index-hash';

function buildIndex(allImages, albumPath) {
	var version = '1';
	var hash = crypto.createHash('md5').update(version + '\n');
	var imgs = [];
	allImages.forEach(function (image) {
		var name = path.basename(image.Key);
		imgs.push({full: name, thumb: image.meta.thumbName});
		hash.update(name);
		hash.update(image.meta.thumbName);
	});
	var object = {
		images: imgs,
	};
	var js = config.AWS.prefix + albumPath + 'index.js';
	return {object: object, hash: hash.digest('hex'), path: js, version: version};
}

function checkIndex(index, callback) {
	s3.head(index.path, function (err, headers) {
		if (err)
			return callback(null, false);
		callback(null, headers[indexHashHeader] == index.hash);
	});
}

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
			thumbs[thumbName] = new Date(thumb.LastModified);
		});
		listings.images.objects.forEach(function (image) {
			if (!path.extname(image.Key).match(config.validExtensions))
				return;
			image.meta = imageMeta(image, dir);
			allImages.push(image);
			var thumbDate = thumbs[image.meta.thumbName];
			if (thumbDate && thumbDate > new Date(image.LastModified))
				return;
			needThumbnails.push(image);
		});
		callback(null, needThumbnails, allImages);
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
		thumbnailConfHash = consistentObjectHash(config.thumbnail);
	var hash = crypto.createHash('md5');
	hash.update(thumbnailConfHash);
	hash.update(info.md5);
	hash = hash.digest('hex').slice(0, 6);
	var prefix = path.basename(image.Key).slice(0, -info.ext.length);
	info.thumbName = prefix + '_' + hash + info.ext;
	info.thumbRemotePath = config.AWS.prefix + 'thumbs/' + albumDir + info.thumbName;

	return info;
}

function isDownloaded(image, callback) {
	fs.stat(image.meta.localPath, function (err) {
		callback(!!err);
	});
}

function downloadImage(image, callback) {
	var tmp = tempFilename(image.meta.ext);
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
	thumbnailImage(image, function (err, tmp) {
		if (err)
			return callback(err);
		uploadThumbnail(tmp, image, function (err) {
			fs.unlink(tmp);
			callback(err);
		});
	});
}

function thumbnailImage(image, callback) {
	console.log("Thumbnailing " + image.meta.localPath + "...");
	var tmp = tempFilename(image.meta.ext);
	var args = [image.meta.localPath];
	args.push('-thumbnail', config.thumbnail.size);
	args.push('-auto-orient');
	args.push('-colorspace', 'sRGB');
	args.push('-strip');
	args.push('-quality', config.thumbnail.quality);
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
	var headers = {
		'Content-Type': 'image/jpeg',
		'x-amz-storage-class': 'REDUCED_REDUNDANCY',
	};
	s3.putFile(dest, localPath, 'public-read', headers, callback);
}

// Helpers

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

function tempFilename(ext) {
	var rand = Math.ceil(Math.random() * 1e12).toString(36);
	return path.join(config.scratchDir, 'tmp_' + rand + ext);
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

function pluralize(n, noun) {
	return n + ' ' + noun + (n==1 ? '' : 's');
}

function objectMD5(obj) {
	return (obj.ETag || obj.etag).replace(/"/g, '').toLowerCase();
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
			async.forEachSeries(garbage, fs.unlink.bind(fs), callback);
		});
	}
}

if (require.main === module) {
	setup(function (err) {
		if (err)
			throw err;
		updateAlbum(config.testAlbum, function (err) {
			if (err)
				throw err;
			console.log("Done.");
		});
	});
}
