var dialog = new BootstrapDialog({
  title: 'Upload local files',
  message: "<div><input type='file' id='file' style='display:block' /></div>",
  size: BootstrapDialog.SIZE_WIDE,
  cssClass: 'file-modal',
  buttons: [
    {
      id: 'upload_local_files_button',
      label: 'Upload local files',
      cssClass: 'btn-primary',
      title: 'Upload local files',
      action: function () {
        document.getElementById('file').click();
        //upload_spreadsheet($('#file').prop('files')[0])
      },
    },
    {
      label: 'Close',
      action: function (dialogItself) {
        dialogItself.close();
      },
    },
  ],
});

//uid = document.location.href;
//uid = uid.split('/');
//uid = uid[uid.length - 2];

$(document).ready(function () {
  //uid = document.location.href
  //uid = uid.split("/")
  //uid = uid[uid.length - 2]
  //******************************Event Handlers Block*************************//
  var component = 'files';
  //var copoVisualsURL = "/copo/copo_visuals/";
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = get_component_meta(component);

  load_records(componentMeta); // call to load component records

  //register_resolvers_event(); //register event for publication resolvers

  //instantiate/refresh tooltips
  refresh_tool_tips();

  //trigger refresh of table
  $('body').on('refreshtable', function (event) {
    do_render_component_table(globalDataBuffer, componentMeta);
  });

  //handle task button event
  $('body').on('addbuttonevents', function (event) {
    do_record_task(event);
  });

  // Runs after each DataTable draw. Used to attach column-header tooltips and
  // compute the aggregate bucket size shown above the search box.
  $('body').on('posttablerefresh', function (event) {
    table = $('#' + componentMeta.tableID).DataTable();
    var numCols = table.columns().nodes().length;

    for (var i = 0; i < numCols; i++) {
      // Attach a tooltip to the Checksum header explaining S3 ETag semantics.
      // For single-part uploads the ETag is a plain MD5; for multipart uploads
      // it is an MD5-of-MD5s suffixed with -N (number of parts) and cannot be
      // used to verify raw file integrity against other systems.
      if ($(table.column(i).header()).text().trim() == 'Checksum') {
        $(table.column(i).header()).attr(
          'title',
          'S3 ETag. For multipart-uploaded files this ends in -N and is not a plain MD5 \u2014 it cannot be used to verify file integrity against other systems. The MD5 used for ENA submission is computed separately when the file is downloaded for transfer.'
        );
      }
      // Sum the raw byte values in the 'Size (bytes)' column to display
      // a human-readable total above the DataTable search box.
      if ($(table.column(i).header()).text().trim() == 'Size (bytes)') {
        var bucket_size_in_GB = table
          .column(i)
          .data()
          .toArray()
          .reduce((accumulator, currentValue) => accumulator + currentValue, 0);

        let table_wrapper = $('#' + componentMeta.tableID + '_wrapper');

        // Create the total-size span once; reuse it on subsequent redraws.
        total_size = table_wrapper.find('#total_size');
        if (total_size.length == 0) {
          $('<span id="total_size"/>')
            .insertBefore(
              table_wrapper.find('.dataTables_filter').find('label')
            )
            .css({ float: 'left', padding: '16px 0' });
          total_size = table_wrapper.find('#total_size');
        }
        total_size.text(
          'Total size for the files: ' + (function(bytes) {
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
            if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
            if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(2) + ' GB';
            if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(2) + ' TB';
            return (bytes / 1125899906842624).toFixed(2) + ' PB';
          })(bucket_size_in_GB)
        );
        break; // No need to inspect remaining columns once size is processed.
      }
    }
  });

  // Remove profile title if present
  // let profileTitleDiv = $('.page-title-custom').find('span.profile-title');
  // if (profileTitleDiv.is(':visible')) profileTitleDiv.remove();

  //details button hover
  /*
    $(document).on("mouseover", ".detail-hover-message", function (event) {
        $(this).prop('title', 'Click to view ' + component + ' details');
    });
    */

  //******************************Functions Block******************************//

  function do_record_task(event) {
    var task = event.task.toLowerCase(); //action to be performed e.g., 'Edit', 'Delete'
    var tableID = event.tableID; //get target table

    //retrieve target records and execute task
    var table = $('#' + tableID).DataTable();
    var records = []; //
    $.map(table.rows('.selected').data(), function (item) {
      records.push(item);
    });

    //add task
    if (task == 'add_files_by_terminal') {
      do_add_record();
    } else if (task == 'add_files_locally') {
      $('#uploadModal').modal('show');
    } else {
      form_generic_task(component, task, records);
    }
  }

  // Open the presigned-URL upload modal in its initial state (input visible,
  // copy button hidden until URLs have been generated).
  function do_add_record() {
    $('#url_upload_controls').show();
    $('#presigned_url_modal').modal('show');
    $('#command_area').html('');
    $('#copy_command_button').fadeOut();
    $('#process_urls_button').fadeIn();
  }

  $(document).on(
    'click',
    '#presigned_urls_modal_button, .new-terminal-file ',
    function (evt) {
      evt.preventDefault();
      do_add_record();
    }
  );

  $(document).on(
    'click',
    '#presigned_urls_modal_button, .new-local-file ',
    function (evt) {
      evt.preventDefault();
      $('#uploadModal').modal('show');
    }
  );

  // Send the pasted file list to the server to generate presigned S3 PUT URLs.
  // The server returns [{name, url}] objects; we build a nohup curl command
  // string the user can copy and run in their terminal to upload the files.
  $(document).on('click', '#process_urls_button', function (evt) {
    // Expect one filename per line (e.g. output of ls -F1).
    var data = $('#url_text_area').val();
    filenames = data.split('\n');
    for (var i = 0; i < filenames.length; i++) {
      filenames[i] = filenames[i].trim();
      // S3 keys with spaces cause curl quoting issues; reject them early.
      if (filenames[i].indexOf(' ') > -1) {
        alert('File name cannot contain spaces');
        return;
      }
    }
    file_names = JSON.stringify(filenames);

    var csrftoken = $.cookie('csrftoken');
    $('#url_upload_controls').hide();
    $('#command_panel').show();
    $('#command_area').html('Please wait ...')
    $.ajax({
      url: '/copo/copo_files/process_urls',
      headers: { 'X-CSRFToken': csrftoken },
      method: 'POST',
      data: { data: file_names },
      dataType: 'json',
    })
      .done(function (d) {
        $('#copy_command_button').fadeIn();
        $('#process_urls_button').fadeOut();
        var inner = '';
        $(d).each(function (idx, obj) {
          inner += "curl -k -T '" + obj.name + "' '" + obj.url + "'; ";
        });
        var out = "<kbd>nohup bash -c \"" + inner + "\" > upload.log 2>&1 &</kbd>";
        $('#command_area').html(out);
        $('#command_panel').show();
      })
      .fail(function (d) {
        $('#command_area').html(d.responseText);
        $('#copy_command_button').fadeOut();
        $('#process_urls_button').fadeIn();
        console.log(d);
      });
  });

  $(document).on('click', '#copy_command_button', function () {
    var btn = $(this);
    navigator.clipboard.writeText($('#command_area').text().trim()).then(function () {
      btn.text('Copied!').prop('disabled', true);
      setTimeout(function () {
        btn.text('Copy command').prop('disabled', false);
      }, 2000);
    });
  });

  $(document).on('click', '#upload_local_files_button', function (evt) {
    $('#uploadModal').find('#file').click();
  });
});

// Upload files selected via the local file picker directly to S3 through the
// Django upload endpoint. Progress is shown as a percentage while the XHR
// transfer is in flight; on completion the files table is refreshed.
function upload_files(files) {
  $('#warning_info').fadeOut('fast');
  $('#warning_info2').fadeOut('fast');

  var csrftoken = $.cookie('csrftoken');
  // Append each file under its index as the field name; the server iterates
  // request.FILES by key to reconstruct the list.
  form = new FormData();
  for (var i = 0; i < files.length; i++) {
    form.append(i.toString(), files[i]);
  }

  $('#upload_local_files_button').fadeOut();
  var percent = $('.percent');
  $('#ss_upload_spinner').fadeIn('fast');
  var profile_id = $('#profile_id').val();

  jQuery
    .ajax({
      url: '/copo/copo_files/upload_ecs_files/' + profile_id,
      data: form,
      files: files,
      cache: false,
      dataType: 'json',
      contentType: false,  // Let the browser set multipart/form-data with boundary.
      processData: false,  // Prevent jQuery serialising the FormData object.
      method: 'POST',
      type: 'POST', // For jQuery < 1.9
      headers: { 'X-CSRFToken': csrftoken },

      // Override the XHR factory to wire up upload progress reporting.
      xhr: function () {
        var xhr = jQuery.ajaxSettings.xhr();
        xhr.upload.onprogress = function (evt) {
          var percentVal = Math.round((evt.loaded / evt.total) * 100);
          percent.html('<b>' + percentVal + '%</b>');
        };
        xhr.upload.onload = function () {
          percent.html('');
          console.log('DONE!');
        };
        return xhr;
      },
    })
    .fail(function (jqXHR, status, error) {
      $('#upload_local_files_button').fadeIn();
      $('#ss_upload_spinner').fadeOut('fast');
      // status 0 means the request was aborted (e.g. browser navigation); use
      // a generic message rather than exposing a raw HTTP error in that case.
      var message = 'Cannot upload files, please check your file size';
      if (jqXHR.status != '0') {
        message = jqXHR.status + ' ' + error;
      }

      BootstrapDialog.show({
        title: 'Error',
        message: message,
        type: BootstrapDialog.TYPE_DANGER,
      });
    })
    .done(function (data) {
      $('#upload_local_files_button').fadeIn();
      $('#ss_upload_spinner').fadeOut('fast');
      $('#uploadModal').modal('hide');
      result_dict = {};
      result_dict['status'] = 'success';
      result_dict['message'] = 'File(s) have been uploaded!';
      do_crud_action_feedback(result_dict);
      globalDataBuffer = data;
      // If the response includes updated table data, trigger a full table redraw.
      if (data.hasOwnProperty('table_data')) {
        var event = jQuery.Event('refreshtable');
        $('body').trigger(event);
      }
    });
}
