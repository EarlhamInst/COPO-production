/*=============================================================
    Author URI: www.binarytheme.com
    License: Commons Attribution 3.0

    http://creativecommons.org/licenses/by/3.0/

    100% To use For Personal And Commercial Use.
    IN EXCHANGE JUST GIVE US CREDITS AND TELL YOUR FRIENDS ABOUT US
   
    ========================================================  */

// (function ($) {
//   'use strict';
//   var mainApp = {
//     main_fun: function () {
//       /*====================================
//              CUSTOM LINKS SCROLLING FUNCTION
//             ======================================*/

//       $("nav a[href*='#']").click(function () {
//         if (
//           location.pathname.replace(/^\//, '') ==
//             this.pathname.replace(/^\//, '') &&
//           location.hostname == this.hostname
//         ) {
//           var $target = $(this.hash);
//           $target =
//             ($target.length && $target) ||
//             $('[name="' + this.hash.slice(1) + '"]');
//           if ($target.length) {
//             var targetOffset = $target.offset().top;
//             $('html,body').animate({ scrollTop: targetOffset }, 800); //set scroll speed here
//             return false;
//           }
//         }
//       });

//       /*====================================
//                 NAV SCRIPTS
//             ======================================*/
//       /*
//             $(window).bind('scroll', function () {
//                 var navHeight = $(window).height() -50;
//                 if ($(window).scrollTop() > navHeight) {
//                     $('nav').addClass('fixed');
//                 }
//                 else {
//                     $('nav').removeClass('fixed');
//                 }
//             });
//             */
//       /*====================================
//                WRITE YOUR SCRIPTS BELOW
//            ======================================*/
//     },

//     initialization: function () {
//       mainApp.main_fun();
//     },
//   };

//   $(document).ready(function () {
//     mainApp.main_fun();
//     var images = [
//       'img3.jpg',
//       'img1.jpg',
//       'img2.jpg',
//       'img4.jpg',
//       'img5.jpg',
//       'img6.jpg',
//       'img7.jpg',
//     ];
//     var idx = Math.round(Math.random() * images.length + 1);
//     var str = 'assets/img/' + images[idx];

//     $('.carousel').carousel({ interval: 5000 });

//     $('#aboutCarousel').data('carousel_counter', idx);
//     var carousel_text = [
//       'share your data with fellow researchers',
//       'get credit for your research objects',
//       'painless data annotation',
//       'easily deposit to public repositories',
//     ];

//     $('#aboutCarousel').on('slide.bs.carousel', function () {
//       var count = $('#aboutCarousel').data('carousel_counter');
//       count = count + 1;
//       // console.log(count)
//       var idx = count % carousel_text.length;
//       //var image_idx = Math.round((Math.random() * images.length) + 1)

//       //console.log(image_idx)
//       $('#carousel_text').html(carousel_text[idx]);
//       $('#aboutCarousel').data('carousel_counter', count++);
//     });

//     // $('#email_submit').on('click', submit_email)
//   });
// })(jQuery);

$(document).ready(function () {
  // Carousel on About page
  initialiseAboutCarousel();

  // Top navigation bar
  setActiveNavItem(); // Set current web page as active nav bar item in top navbar

  $("nav a[href*='#']").click(function () {
    if (
      location.pathname.replace(/^\//, '') ==
      this.pathname.replace(/^\//, '') &&
      location.hostname == this.hostname
    ) {
      var $target = $(this.hash);
      $target =
        ($target.length && $target) || $('[name="' + this.hash.slice(1) + '"]');
      if ($target.length) {
        var targetOffset = $target.offset().top;
        $('html,body').animate({ scrollTop: targetOffset }, 800); //set scroll speed here
        return false;
      }
    }
  });

  // Populate current tasks popover in the top navigation bar
  $('#bell_notification').popover({
    html: true,
    content: function () {
      const created = 'Not available on this page';
      const latest_message = 'No message loaded';

      return `
        <div>
          <div>
            Message Received:<br/>${created}
          </div>
          <br/>
          <b>${latest_message}</b>
        </div>
      `;
    },
  });
});

function initialiseAboutCarousel() {
  let $items = $('#aboutCarousel .item');
  let numImageItems = $items.length;

  let currentIndex = Math.floor(Math.random() * numImageItems);
  $items.eq(currentIndex).addClass('active');

  function nextSlide() {
    let $current = $items.eq(currentIndex);
    $current.removeClass('active');

    currentIndex = (currentIndex + 1) % numImageItems;
    $items.eq(currentIndex).addClass('active');
  }

  setInterval(nextSlide, 5000);
}

function setActiveNavItem() {
  const activePage = window.location.href;

  $('.nav li a').filter(function () {
    let linkPage = `${this.href}/`;
    // home page i.e. COPO front page URL will end with '//' so it needs to be ommitted
    linkPage = linkPage.endsWith('//') ? this.href : linkPage;

    if (activePage === linkPage) {
      $(this).parent().addClass('active');
      $(this).parent().append('<span class="sr-only">(current)</span>');
    }
  });
}
