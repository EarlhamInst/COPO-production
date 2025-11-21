$(document).ready(function () {
  initialiseNavToggle();

  $(document).on('click', '.card', function () {
    window.location = '/copo/tol_dashboard/stats#';
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

  const image = getRandomInt(images.length);
  $('body').css('background-image', 'url(' + images[image] + ')');

  try {
    var colour = getRandomInt(content_classes.length);
    $('#main_banner').addClass(content_classes[colour]);
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
  const $nav = $('#publicNavbar, #authNavbar');

  $nav.find('.navbar-toggle').on('click', function () {
    $(this).closest('nav').find('.navbar-nav').toggleClass('active');
  });
  // Handle dropdown toggles
  $nav
    .find('.navbar-nav li.dropdown > a.dropdown-toggle')
    .on('click', function (e) {
      e.preventDefault(); // Prevent default for toggle
      e.stopPropagation(); // Stop event bubbling

      const $li = $(this).closest('li');
      $li.siblings().removeClass('open'); // Close other dropdowns
      $li.toggleClass('open'); // Toggle current dropdown
    });

  // Handle link clicks (including links inside dropdowns)
  $nav
    .find('.navbar-nav li a')
    .not('.dropdown-toggle')
    .on('click', function () {
      if ($(window).width() < 768) {
        // Collapse menu on small screens after navigation
        $(this).closest('.navbar-nav').removeClass('active');
      }
    });

  // Reset menu on window resize
  $(window).on('resize', function () {
    if ($(window).width() >= 768) {
      $nav.find('.navbar-nav').removeClass('active');
      $nav.find('.navbar-nav li.dropdown').removeClass('open');
    }
  });
}
