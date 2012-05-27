(function () {

var state = {};
var loading = false;
var $main;

function orientSelf() {
	// Figure out where the album root is based on the JS script src
	var $script = $('script:last');
	state.rootPath = $script.attr('src').match(/(.*)gallery\.js$/)[1];
	var absRoot = $script.prop('src').match(/(.*)gallery\.js$/)[1];
	state.path = dirname(removePrefix(absRoot, document.location.href));
}

function renderAlbum(album) {
	$main.empty();
	$.each(album.dirs, function (i, dir) {
		var $a = $('<a/>').attr('href', dir.path).text(dir.path);
		$main.append($('<div/>').append($a));
	});
	$.each(album.images, function (i, image) {
		var $img = $('<img>').attr('src', currentDir(image.thumb));
		var $a = $('<a/>').attr('href', imageDir(image.full)).append($img);
		$main.append($('<figure/>').append($a));
	});
}

function addStyles() {
	if ($('#thumb-style').length)
		return;
	$('<link rel=stylesheet>').attr('href', state.rootPath + 'plain.css').appendTo('head');
	var dims = config.thumbnail.size.match(/^(\d+)x(\d+)$/);
	function halfSize(i) {
		return Math.floor(parseInt(dims[i], 10));
	}
	// Use configuration's thumbnail dimensions for cell size and vertical alignment hack
	var rule = 'width: ' + halfSize(1) + 'px; height: ' + halfSize(2) + 'px; line-height: ' + halfSize(2) + 'px;';
	$('<style id="thumb-style">div, figure { ' + rule + ' }</style>').appendTo('head');
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
	$main = $('body');
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

$(document).on('click', 'div a', function (event) {
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
