$(document).on("document_ready", function() {
  profile_id = $('#profile_id').val();
  //var uid = document.location.href;
  //uid = uid.split('/');
  //uid = uid[uid.length - 1];
  var wsprotocol = 'ws://';
  var s3socket;
  $('#copy_urls_button').fadeOut();
  $('#process_urls_button').fadeIn();

  if (window.location.protocol === 'https:') {
    wsprotocol = 'wss://';
  }
  var wsurl = wsprotocol + window.location.host + '/ws/assembly_status/' + profile_id;

  s3socket = new WebSocket(wsurl);

  s3socket.onclose = function (e) {
    console.log('s3socket closing ', e);
  };
  s3socket.onopen = function (e) {
    console.log('s3socket opened ', e);
  };
  s3socket.onmessage = function (e) {
    d = JSON.parse(e.data);
    const { $el: $element, inModal: isModalVisible } = getAlertElement(
      d.html_id
    );
    const rawMessage = d.message;
    const hasMessage =
      typeof rawMessage === 'string' && rawMessage.trim().length > 0;
    const message = hasMessage ? rawMessage.trim() : '';

    // Only show an alert if a message exists
    if (hasMessage) {
      // Dismiss helper content if applicable
      hideModalInstructionText(message, d.action);

      if (isModalVisible) {
        // If modal is visible then, show an alert inside it
        const allAlertClasses = Object.values(alertClassMap).join(' ');
        $element
          .html(message)
          .removeClass(allAlertClasses)
          .addClass(alertClassMap[d.action] || 'alert-info')
          .fadeIn(50);
      } else if (d.action && $element.length) {
        // else, show an alert message within the 'Info' sidebar tab on the page
        displayAlert(d.action, message);
      }
    }
  };
  window.addEventListener('beforeunload', function (event) {
    s3socket.close();
  });
});

function upload_assembly_files() {
  var csrftoken = $.cookie('csrftoken');
  var profile_id = $('#profile_id').val();
  var fieldset = $('#assembly_form input, textarea, select');
  const form = new FormData();
  var count = 0;
  var files = [];
  $(fieldset).each(function (idx, el) {
    if (el.type == 'file') {
      form.append(el.name, el.files[0]);
    } else {
      form.append(el.name, el.value);
    }
  });
  $('#assembly_form input, textarea, select').prop('disabled', true);

  form.append('profile_id', profile_id);
  jQuery
    .ajax({
      url: '/copo/copo_assembly/' + profile_id,
      data: form,
      files: files,
      cache: false,
      contentType: false,
      processData: false,
      type: 'POST', // For jQuery < 1.9
      headers: {
        'X-CSRFToken': csrftoken,
      },
    })
    .fail(function (data) {
      $('#assembly_form input, textarea, select').prop('disabled', false);
      $('#id_study').prop('disabled', true);
      $('#loading_span').fadeOut();
      console.log(data);
      BootstrapDialog.show({
        title: 'Error',
        message: data.responseText,
        type: BootstrapDialog.TYPE_DANGER,
      });
    })
    .done(function (data) {
      $('#submit_assembly_button').fadeOut();
      $('#assembly_form').hide();
      $('#loading_span').hide();
      //$("input").fadeOut()
      //$("select").fadeOut()
      //$("textarea").fadeOut()
      //
      console.log(data);
    });
}

function doPost() {
  var evt = window.event;
  evt.preventDefault();

  upload_assembly_files();
  // $("#submit_assembly_button").fadeOut()
  // $("#loading_span").fadeIn()
  // $('#assembly_form').submit()
  // var fieldset = $("#assembly_form input, textarea, select").prop("disabled", true)
}
