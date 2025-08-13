function get_checklist_id() {
  if ($('#checklist_id').length > 0) {
    return $('#checklist_id').find(':selected').val();
  } else {
    return 'read';
  }
}

function initialise_checklist_id() {
  if ($('#checklist_id').length > 0) {
    first = true;
    var profile_checklist_ids = document.getElementById(
      'profile_checklist_ids'
    ).value;
    $('#checklist_id option').each(function () {
      if (profile_checklist_ids.includes($(this).val())) {
        $(this).text('* ' + $(this).text());
        if (first) {
          $(this).prop('selected', true);
          first = false;
        }
      }
    });
  }
}

var columnDefs = [];

var dialog = new BootstrapDialog({
  title: 'Upload Sample Manifest',
  message: "<div><input type='file' id='fileid' style='display:none' /></div>",
  size: BootstrapDialog.SIZE_WIDE,
  buttons: [
    {
      id: 'upload_sample_manifest_button',
      label: 'Upload Sample Manifest',
      cssClass: 'btn-primary',
      title: 'Upload Sample Manifest',
      action: function () {
        document.getElementById('file').click();
        //upload_spreadsheet($('#file').prop('files')[0])
      },
    },
    {
      id: 'save_sample_button',
      label: 'Finish',
      cssClass: 'btn-primary',
      title: 'Finish',
      disabled: true,
      action: function () {
        var $button = this; // 'this' here is a jQuery object that wrapping the <button> DOM element.
        $button.disable();
        $button.spin();
        dialog.setClosable(false);
        save_sample_data();
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

$(document).on('document_ready', function () {
  profile_id = $('#profile_id').val();
  //var uid = document.location.href;
  //uid = uid.split('/');
  //uid = uid[uid.length - 2];
  var wsprotocol = 'ws://';
  var s3socket;

  if (window.location.protocol === 'https:') {
    wsprotocol = 'wss://';
  }
  var read_wsurl =
    wsprotocol + window.location.host + '/ws/read_status/' + profile_id;
  var submission_wsurl =
    wsprotocol + window.location.host + '/ws/submission_status/' + profile_id;
  s3socket = new WebSocket(read_wsurl);
  submissionSocket = new WebSocket(submission_wsurl);

  submissionSocket.onclose = function (e) {
    console.log('submissionSocket closing ', e);
  };
  submissionSocket.onopen = function (e) {
    console.log('submissionSocket opened ', e);
  };

  submissionSocket.onmessage = function (e) {
    d = JSON.parse(e.data);
    var element = '';

    if (d.html_id != '') {
      element = element = $('#' + d.html_id);

      if (!d && !$(element).is(':hidden')) {
        $(element).fadeOut('50');
      } else if (d && d.message && $(element).is(':hidden')) {
        $(element).fadeIn('50');
      }
    }

    //$("#" + d.html_id).html(d.message)
    if (d.action === 'info') {
      // show something on the info div
      // check info div is visible
      $(element).removeClass('alert-danger').addClass('alert-info');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    } else if (d.action === 'warning') {
      // show something on the info div
      // check info div is visible
      $(element).removeClass('alert-danger').addClass('alert-warning');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    } else if (d.action === 'error') {
      // check info div is visible
      $(element).removeClass('alert-info').addClass('alert-danger');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    }
  };

  s3socket.onclose = function (e) {
    console.log('s3socket closing ', e);
  };
  s3socket.onopen = function (e) {
    console.log('s3socket opened ', e);
  };

  s3socket.onmessage = function (e) {
    d = JSON.parse(e.data);
    var element = '';

    if (d.html_id != '') {
      element = element = $('#' + d.html_id);
      if ($('.modal-dialog').is(':visible')) {
        elem = $('.modal-dialog').find('#' + d.html_id);
        if (elem) {
          element = elem;
        }
      }

      if (!d && !$(element).is(':hidden')) {
        $(element).fadeOut('50');
      } else if (d && d.message && $(element).is(':hidden')) {
        $(element).fadeIn('50');
      }
    }

    //$("#" + d.html_id).html(d.message)
    if (d.action === 'info') {
      // show something on the info div
      // check info div is visible
      $(element).removeClass('alert-danger').addClass('alert-info');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    } else if (d.action === 'warning') {
      // show something on the info div
      // check info div is visible
      $(element).removeClass('alert-danger').addClass('alert-warning');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    } else if (d.action === 'error') {
      // check info div is visible
      $(element).removeClass('alert-info').addClass('alert-danger');
      $(element).html(d.message);
      //$("#spinner").fadeOut()
    } else if (d.action === 'make_table') {
      // make table of metadata parsed from spreadsheet
      if ($.fn.DataTable.isDataTable('#sample_parse_table')) {
        $('#sample_parse_table').DataTable().clear().destroy();
      }
      $('#sample_parse_table').find('thead').empty();
      $('#sample_parse_table').find('tbody').empty();
      var body = $('tbody');
      var count = 0;
      for (r in d.message) {
        row = d.message[r];
        var tr = $('<tr/>');
        for (c in row) {
          cell = row[c];
          if (count === 0) {
            var td = $('<th/>', {
              html: cell,
            });
          } else {
            var td = $('<td/>', {
              html: cell,
            });
          }
          tr.append(td);
        }
        if (count === 0) {
          $('#sample_parse_table').find('thead').append(tr);
        } else {
          $('#sample_parse_table').find('tbody').append(tr);
        }
        count++;
      }
      $('#sample_info').hide();
      $('#sample_parse_table').DataTable({
        scrollY: 'auto',
        scrollX: true,
      });
      $('#table_div').fadeIn(1000);
      $('#sample_parse_table').DataTable().draw();
      $('#tabs').fadeIn();
      $('#ena_finish_button').fadeIn();
    } else if (d.action === 'refresh_table') {
      $(element).removeClass('alert-danger').addClass('alert-info');
      $(element).html(d.message);
      var args_dict = {};
      args_dict['sample_checklist_id'] = get_checklist_id();
      (args_dict['profile_id'] = $('#profile_id').val()),
        load_records(componentMeta, args_dict, columnDefs); // call to load component records
    } else if (d.action === 'file_processing_status') {
      $(element).html(d.message);
      table = $('#sample_table').DataTable();
      //clear old, set new data
      table.rows().deselect();
      table.clear().draw();
      table.rows.add(d.data['table_data']).draw();
      table.columns.adjust().draw();
      table.search('').columns().search('').draw();
      $(element).html(d.message + ' ... Done');

      /*
        table = $('#read_table').DataTable();
        table.columns.adjust().draw();
         for (var i = 0; i < d.data["file_processing_status"].length; i++) {
          	
          let rows = table.rows((idx, data) => data.run_accession === d.data["file_processing_status"][i]["run_accession"]);
          if (rows.count() > 0) {
            rows.every(function (rowIdx, tableLoop, rowLoop) {
              var row = this.data();
              row["ena_file_processing_status"] = d.data["file_processing_status"][i]["msg"];
              if (d.data["file_processing_status"][i]["msg"] != "" && !d.data["file_processing_status"][i]["msg"].includes("File archived")) {
                $(table.row(this.index()).node()).addClass("highlight_error_file_processing_status")
              }
              this.data(row).draw();
            });
          }  

          }
          */
    }
  };
  window.addEventListener('beforeunload', function (event) {
    s3socket.close();
    submissionSocket.close();
  });

  //******************************Event Handlers Block*************************//
  var component = 'general_sample';
  var copoFormsURL = '/copo/copo_forms/';
  //var copoVisualsURL = "/copo/copo_visuals/";
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = get_component_meta(component);

  //load_records(componentMeta); // call to load component records

  initialise_checklist_id();
  var args_dict = { sample_checklist_id: get_checklist_id() };
  $('.download-blank-manifest-template').attr(
    'href',
    $('#blank_manifest_url_' + get_checklist_id()).val()
  );
  (args_dict['profile_id'] = $('#profile_id').val()),
    load_records(componentMeta, args_dict, columnDefs); // call to load component records

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

  // Close the alert message once the close button is clicked
  // since the boostrap event is strangely not registered to do so
  $(document).on('click', '#page_alert_panel .alert .close', function () {
    $(this).closest('.alert').remove();
  });

  //add new component button
  $(document)
    .off('click', '.new-general-sample-spreadsheet-template')
    .on('click', '.new-general-sample-spreadsheet-template', function (event) {
      url =
        '/copo/copo_sample/sample_manifest_validate/' +
        profile_id +
        '?checklist_id=' +
        get_checklist_id();
      dialog.realize();
      dialog.setMessage($('<div></div>').load(url));
      dialog.open();
      dialog.getButton('save_sample_button').disable();

      $('.modal-dialog')
        .find('#file')
        .off('change')
        .on('change', function (event) {
          if (
            $(this).prop('files') == undefined ||
            $(this).prop('files').length == 0
          ) {
            return;
          }
          dialog.getButton('upload_sample_manifest_button').disable();
          dialog.getButton('upload_sample_manifest_button').spin();
          dialog.setClosable(false);
          upload_spreadsheet($(this).prop('files')[0]);
        });
    });

  $('#checklist_id').change(function () {
    if ($.fn.dataTable.isDataTable('#' + componentMeta.tableID)) {
      //if table instance already exists, then do refresh
      table = $('#' + componentMeta.tableID).DataTable();
      table.clear().destroy();
      $('#' + componentMeta.tableID).empty();
    }
    $('.download-blank-manifest-template').attr(
      'href',
      $('#blank_manifest_url_' + this.value).val()
    );
    var args_dict = {};
    args_dict['sample_checklist_id'] = this.value;
    args_dict['profile_id'] = $('#profile_id').val();
    load_records(componentMeta, args_dict, columnDefs); // call to load component records
  });

  // Set colour of 'help_add_button' button and 'new-samples-spreadsheet-template'
  // button based on profile type
  var profile_type = document
    .getElementById('profile_type')
    .value.toLowerCase();
  var colour = profile_type_def[profile_type]['widget_colour'];
  $('#help_add_button').css('color', 'white').css('background-color', colour);
  $('.new-general-sample-spreadsheet-template')
    .css('color', 'white')
    .css('background-color', colour);
  /*
  if (document.getElementById('profile_type').value.includes('ASG')) {
    $('#help_add_button').addClass('violet');
    $('.new-reads-spreadsheet-template').addClass('violet');
  } else if (document.getElementById('profile_type').value.includes('DTOL')) {
    $('#help_add_button').addClass('green');
    $('.new-reads-spreadsheet-template').addClass('green');
  } else if (document.getElementById('profile_type').value.includes('ERGA')) {
    $('#help_add_button').addClass('pink');
    $('.new-reads-spreadsheet-template').addClass('pink');
  } else if (
    document.getElementById('profile_type').value.includes('Stand-alone')
  ) {
    $('#help_add_button').addClass('teal active');
    $('.new-reads-spreadsheet-template').addClass('teal active');
  } else {
    // Button colour for other profile types
    // $('#help_add_button').addClass('green');
    // $('.new-reads-spreadsheet-template').addClass('green');
  }
*/
  //details button hover
  /*
    $(document).on("mouseover", ".detail-hover-message", function (event) {
        $(this).prop('title', 'Click to view ' + component + ' details');
    });
    */

  //$(".new-reads-spreadsheet-template").addClass("btn btn-info").attr("data-toggle", "modal").attr("data-target", "#uploadModal")

  //******************************Functions Block******************************//

  function do_record_task(event) {
    var task = event.task.toLowerCase(); //action to be performed e.g., 'Edit', 'Delete'
    var tableID = event.tableID; //get target table
    var profile_id = $('#profile_id').val();
    //retrieve target records and execute task
    var table = $('#' + tableID).DataTable();
    var records = []; //
    $.map(table.rows('.selected').data(), function (item) {
      records.push(item);
    });

    var args_dict = { sample_checklist_id: get_checklist_id() };

    // Download sample manifest
    if (task == 'download-sample-manifest') {
      $('#download-sample-manifest-link').attr(
        'href',
        '/copo/copo_sample/download_manifest/' +
          profile_id +
          '/' +
          get_checklist_id()
      );
      $('#download-sample-manifest-link span').trigger('click');
      return;
    } else {
      form_generic_task('sample', task, records, args_dict);
    }
  }

  $('body').on('posttablerefresh', function (event) {
    table = $('#' + component + '_table').DataTable();
    //var numCols = $('#' + component + '_table thead th').length;
    var numCols = table.columns().nodes().length;
    table.rows().nodes().to$().addClass('highlight_accession');

    for (var i = 0; i < numCols; i++) {
      if ($(table.column(i).header()).text() == 'STATUS') {
        var no_accessiion_indexes = table
          .rows()
          .eq(0)
          .filter(function (rowIdx) {
            return table.cell(rowIdx, i).data() != 'accepted' ? true : false;
          });
        table
          .rows(no_accessiion_indexes)
          .nodes()
          .to$()
          .addClass('highlight_no_accession');
      }
      if ($(table.column(i).header()).text() == 'ENA FILE PROCESSING STATUS') {
        var error = table
          .rows()
          .eq(0)
          .filter(function (rowIdx) {
            file_processing_status = table.cell(rowIdx, i).data();
            if (
              file_processing_status == '' ||
              file_processing_status.includes('File archived')
            )
              return false;
            else return true;
          });
        table
          .rows(error)
          .nodes()
          .to$()
          .addClass('highlight_error_file_processing_status');
      }
    }
    /*
    $('.ena-accession').each(function (i, obj) {
      if ($(obj).prop('tagName') != 'TH' && $(obj).text() != '') {
        $(obj).html(
          "<a href='https://www.ebi.ac.uk/ena/browser/view/" +
            $(obj).text() +
            "' target='_blank'>" +
            $(obj).text() +
            '</a>'
        );
      }
    });
    */
  });
});

function upload_spreadsheet(file) {
  $('#warning_info').fadeOut('fast');
  $('#warning_info2').fadeOut('fast');
  var csrftoken = $.cookie('csrftoken');
  form = new FormData();
  form.append('checklist_id', get_checklist_id());
  form.append('file', file);
  var percent = $('.percent');
  jQuery
    .ajax({
      url: '/copo/copo_sample/parse_sample_spreadsheet/',
      data: form,
      cache: false,
      contentType: false,
      processData: false,
      method: 'POST',
      type: 'POST', // For jQuery < 1.9
      headers: { 'X-CSRFToken': csrftoken },
      xhr: function () {
        var xhr = jQuery.ajaxSettings.xhr();
        xhr.upload.onprogress = function (evt) {
          var percentVal = Math.round((evt.loaded / evt.total) * 100);
          percent.html('<b>' + percentVal + '%</b>');
          console.log('progress', percentVal);
        };
        xhr.upload.onload = function () {
          percent.html('');
          console.log('DONE!');
        };
        return xhr;
      },
    })
    .fail(function (data) {
      dialog.getButton('upload_sample_manifest_button').enable();
      dialog.setClosable(true);
      dialog.getButton('upload_sample_manifest_button').stopSpin();
      console.log(data);
      responseText = data.responseText;
      if (responseText != '') {
        BootstrapDialog.show({
          title: 'Error',
          message: 'Error ' + data.status + ': ' + data.responseText,
        });
      }
    })
    .done(function (data) {
      // Hide upload button if 'Finish' button is visible and upload was successful
      dialog
        .getButton('upload_sample_manifest_button')
        .stopSpin()
        .disable()
        .hide();
      dialog.getButton('save_sample_button').enable();
      dialog.setClosable(true);
    });
}

function save_sample_data() {
  $.ajax({
    url: '/copo/copo_sample/save_sample_records',
  })
    .fail(function (data) {
      dialog.getButton('upload_sample_manifest_button').enable();
      dialog.setClosable(true);
      dialog.getButton('upload_sample_manifest_button').stopSpin();
      console.log(data);
      responseText = data.responseText;
      if (responseText != '') {
        BootstrapDialog.show({
          title: 'Error',
          message: 'Error ' + data.status + ': ' + data.responseText,
        });
      }
    })
    .done(function (data) {
      result_dict = {};
      result_dict['status'] = 'success';
      result_dict['message'] = 'Sample records are saved';
      do_crud_action_feedback(result_dict);
      dialog.close();
      globalDataBuffer = data;

      if (data.hasOwnProperty('table_data')) {
        var event = jQuery.Event('refreshtable');
        $('body').trigger(event);
      }
    });
}
