// Loading dots/animated ellipsis jQuery plugin
(function ($) {
  $.animatedEllipsis = function (el, options) {
    var base = this;

    base.$el = $(el);

    base.$el.data('animatedEllipsis', base);

    base.dotItUp = function ($element, maxDots) {
      if ($element.text().length == maxDots) {
        $element.text('');
      } else {
        $element.append('.');
      }
    };

    base.stopInterval = function () {
      clearInterval(base.theInterval);
    };

    base.init = function () {
      if (typeof speed === 'undefined' || speed === null) speed = 300;
      if (typeof maxDots === 'undefined' || maxDots === null) maxDots = 3;

      base.speed = speed;
      base.maxDots = maxDots;

      base.options = $.extend({}, $.animatedEllipsis.defaultOptions, options);

      base.$el.html('<span>' + base.options.word + '<em></em></span>');

      base.$dots = base.$el.find('em');
      base.$loadingText = base.$el.find('span');

      base.$el.css('position', 'relative');
      base.$loadingText.css({
        position: 'relative', //'absolute',
        top: base.$el.outerHeight() / 2 - base.$loadingText.outerHeight() / 2,
        // left: base.$el.width() / 2 - base.$loadingText.width() / 2,
      });

      base.theInterval = setInterval(
        base.dotItUp,
        base.options.speed,
        base.$dots,
        base.options.maxDots
      );
    };

    base.init();
  };

  $.animatedEllipsis.defaultOptions = {
    speed: 300,
    maxDots: 3,
    word: 'Loading',
  };

  $.fn.animatedEllipsis = function (options) {
    if (typeof options == 'string') {
      var safeGuard = $(this).data('animatedEllipsis');
      if (safeGuard) {
        safeGuard.stopInterval();
      }
    } else {
      return this.each(function () {
        new $.animatedEllipsis(this, options);
      });
    }
  };
})(jQuery);
// End of 'animatedEllipsis' jquery plugin
