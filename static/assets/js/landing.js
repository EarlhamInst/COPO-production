$(document).ready(function () {
  $(document).on('click', '.card', function () {
    window.location = '/copo/tol_dashboard/stats#';
  });

  // $('.ui.dropdown').dropdown();
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
  if ($('.gray-theme').is(':visible') && $('#contact-sec').is(':visible')) {
    contact_form_label.addClass('white-text');
  } else {
    contact_form_label.removeClass('white-text');
  }
});

function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max));
}
