(function () {

var state = {};
var loading = false;
var $main;
var lightbox;

function orientSelf() {
	// Figure out where the album root is based on the JS script src
	var $script = $('script:last');
	state.rootPath = $script.attr('src').match(/(.*)gallery\.js$/)[1];
	var absRoot = $script.prop('src').match(/(.*)gallery\.js$/)[1];
	state.path = dirname(removePrefix(absRoot, document.location.href));

	var options = new LightboxOptions;
	options.fileLoadingImage = state.rootPath + 'lightbox/loading.gif';
	options.fileCloseImage = state.rootPath + 'lightbox/close.png';
	options.resizeDuration = options.fadeDuration = 250;
	lightbox = new Lightbox(options);
}

function renderAlbum(album) {
	$main.empty();
	$.each(album.dirs, function (i, dir) {
		var $a = $('<a/>').attr('href', dir.path).text(dir.path);
		$main.append($('<div/>').append($a));
	});
	$.each(album.images, function (i, image) {
		var $img = $('<img>').attr('src', currentDir(image.thumb));
		var $a = $('<a/>', {
			href: currentDir(image.med),
			rel: 'lightbox[album]',
		}).append($img);
		$main.append($('<figure/>').append($a));
	});
}

function addStyles() {
	if ($('#thumb-style').length)
		return;
	var dims = config.thumb.size;
	function adjustSize(size) {
		if (window.devicePixelRatio > 1)
			size /= window.devicePixelRatio;
		return Math.floor(size);
	}
	// Use configuration's thumbnail dimensions for cell size and vertical alignment hack
	var width = adjustSize(dims[0]), height = adjustSize(dims[1]);
	var rule = 'width: ' + width + 'px; height: ' + height + 'px; line-height: ' + height + 'px;';
	$('<style id="thumb-style">section div, section figure, section img { ' + rule + ' }</style>').appendTo('head');
}

function requestIndex() {
	if (loading)
		return;
	$.ajax({url: currentDir('index.js'), dataType: 'json',
			success: onGotIndex, error: onError,
			complete: function () { loading = false; },
	});
	loading = true;
}

function currentDir(file) {
	return state.rootPath + state.path + file;
}

function imageDir(file) {
	return state.rootPath + '../' + state.path + file;
}

function initialSetup() {
	// Only necessary if there's no existing history state
	if (loading)
		return;
	orientSelf();
	addStyles();
	requestIndex();
}

$(function () {
	$main = $('section');
	setTimeout(initialSetup, 50);
});

function onGotIndex(album, status, $xhr) {
	state.album = album;
	history.replaceState(state, null, state.rootPath + state.path);
	renderAlbum(album);
}

function onError($xhr, status, err) {
	$main.empty();
	$('<strong/>').text(err).appendTo('body');
}

$(document).on('click', 'section div a', function (event) {
	var path = $(event.target).attr('href');
	state.path += path;
	state.album = null;
	history.pushState(state, null, state.rootPath + state.path);
	requestIndex();
	return false;
});

window.onpopstate = function (event) {
	if (!event.state || event.state.rootPath === undefined)
		return;
	state = event.state;
	addStyles();
	if (state.album)
		renderAlbum(state.album);
	else
		requestIndex();
};

function removePrefix(prefix, str) {
	return str.slice(0, prefix.length) == prefix ? str.slice(prefix.length) : str;
}

function dirname(path) {
	var i = path.lastIndexOf('/');
	return i >= 0 ? path.slice(0, i + 1) : '';
}

})();
