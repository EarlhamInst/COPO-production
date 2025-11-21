$(document).ready(function () {
  initialiseNavToggle();

  $(document).on('click', '.card', function () {
    window.location = '/copo/tol_dashboard/stats#';
  });

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

  const image = getRandomInt(images.length);
  $('body').css('background-image', 'url(' + images[image] + ')');

  try {
    var color = getRandomInt(content_classes.length);
    $('#main_banner').addClass(content_classes[color]);
  } catch (err) {}

  $.getJSON('/api/stats/numbers')
    .done(function (data) {
      $('#num_samples').html(data.samples);
      $('#num_profiles').html(data.profiles);
      $('#num_users').html(data.users);
      $('#num_uploads').html(data.datafiles);
    })
    .fail(function (data) {
      console.log(data);
    });

  // Set current web page as active nav bar item in top navbar
  $('.nav li a').filter(function () {
    const activePage = window.location.href;
    let linkPage = `${this.href}/`;

    // The home page of the website i.e. COPO front page URL will end
    // with '//' so it needs to be omitted
    linkPage = linkPage.endsWith('//') ? this.href : linkPage;

    if (activePage === linkPage) {
      $(this).parent().addClass('active');
      $(this).parent().append('<span class="sr-only">(current)</span>');
    }
  });

  // Set text colour according to the current theme on the 'Contact' web page
  let contact_form_label = $('#email_form').find('label');
  if ($('.grey-theme').is(':visible') && $('#contact-sec').is(':visible')) {
    contact_form_label.addClass('white-text');
  } else {
    contact_form_label.removeClass('white-text');
  }
});

function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max));
}

function initialiseNavToggle() {
  // Initialise navigation bar on the Landing page
  $('#frontpageNav .navbar-toggle').on('click', function () {
    $(this).closest('nav').find('.navbar-nav').toggleClass('active');
  });

  // Handle nav item clicks
  $('#frontpageNav .navbar-nav li a').on('click', function (e) {
    const $li = $(this).closest('li');

    if ($li.hasClass('dropdown')) {
      // Prevent the dropdown toggle from collapsing the menu
      e.preventDefault();
      e.stopPropagation();
      $li.siblings().removeClass('open');
      $li.toggleClass('open');
    } else {
      if ($(window).width() < 768) {
        $(this).closest('.navbar-nav').removeClass('active');
      }
    }
  });

  // Reset menu on window resize
  $(window).on('resize', function () {
    if ($(window).width() >= 768) {
      $('.navbar-nav').removeClass('active');
    }
  });
}
