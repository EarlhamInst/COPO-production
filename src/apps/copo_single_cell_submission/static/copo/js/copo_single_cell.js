var current_study_id = '';

function get_checklist_id() {
  if ($('#checklist_id').length > 0) {
    return $('#checklist_id').find(':selected').val();
  } else {
    return 'singlecell';
  }
}

function get_selected_checklist_name() {
  if ($('#checklist_id').length > 0) {
    return $('#checklist_id').find(':selected').html();
  } else {
    return 'singlecell';
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

function reset_value() {
  $("#singlecell_data_tabs").empty();
  $("#singlecell_data_tab_content").empty();
  current_study_id = '';
}

var columnDefs = [];

$(document).on('document_ready', function () {
  var uid = document.location.href;
  uid = uid.split('/');
  uid = uid[uid.length - 2];
  var wsprotocol = 'ws://';
  var s3socket;
 
  if (window.location.protocol === 'https:') {
    wsprotocol = 'wss://';
  }
  var wsurl = wsprotocol + window.location.host + '/ws/singlecell_status/' + uid;

  s3socket = new WebSocket(wsurl);

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
 
        tabs = $('#singlecell-tabs');
        tab_content = $('#singlecell-tab-content');
        tabs.empty();
        tab_content.empty();
        is_first_compoent = true
        d.data["components"].forEach(component => {
          li = $('<li/>').addClass('nav-item');
          a = $('<a/>').addClass('nav-link')
              .attr('id', component + '-tab')
              .attr('data-toggle', 'tab')
              .attr('href', '#' + component)
              .attr('role', 'tab')
              .attr('aria-controls', component)
              .attr('aria-selected', 'false')
              .html(component.replace(/_/g, ' ').toUpperCase());

          li.append(a);
          tabs.append(li);

          table_name = "singlecell_parse_table_" + component;
          div = $('<div/>')
                .addClass('tab-pane')
                .addClass('fade')
                .attr('id', component)
                .attr('role', 'tabpanel')
                .attr('aria-labelledby', component + '-tab');
          table = $('<table/>')
                  .attr('id', table_name)
                  .addClass('table')
                  .addClass('table-striped')
                  .addClass('table-bordered');
          thead = $('<thead/>');
          tbody = $('<tbody/>');
          table.append(thead);
          table.append(tbody);
          div.append(table);
          tab_content.append(div);

          if (is_first_compoent) {
            li.addClass('active');
            div.addClass('active in');
            is_first_compoent = false;
          } 
 
        var is_first_row = true;
        for (r in d.message[component]) {
          row = d.message[component][r];
          var tr = $('<tr/>');
          for (c in row) {
            cell = row[c];
            var td = $('<td/>', {
              html: cell,
            });
            if (is_first_row) {
                td = $('<th/>', {
                html: cell.replace(/_/g, ' ').toUpperCase(),
              });
            } 
            tr.append(td);
          }
          if (is_first_row) {
            thead.append(tr);
          } else {
            table.append(tr);
          }
          is_first_row = false
        }

        table.DataTable({
         scrollY: 'auto',
         scrollX: true,
       }).draw();
      });

      $('#singlecell_info').hide();
      //$('#singlecell-tabs').fadeIn();
      $('#table_div').fadeIn(1000);
      $('#ena_finish_button').fadeIn();
    } else if (d.action === 'refresh_table') {
      $(element).removeClass('alert-danger').addClass('alert-info');
      $(element).html(d.message);
      var args_dict = {};
      args_dict['singlecell_checklist_id'] = get_checklist_id();
      args_dict['profile_id'] = $('#profile_id').val(),
      load_records(componentMeta, args_dict, columnDefs); // call to load component records  
    } else if (d.action === 'file_processing_status') {
      $(element).html(d.message);
      table = $('#singlecell_table').DataTable();
      //clear old, set new data
      table.rows().deselect();
      table.clear().draw();
      table.rows.add(d.data['table_data']).draw();
      table.columns.adjust().draw();
      table.search('').columns().search('').draw();
      $(element).html(d.message + ' ... Done');

    }
  };
  window.addEventListener('beforeunload', function (event) {
    s3socket.close();
  });

  //******************************Event Handlers Block*************************//
  var component = 'singlecell';
  var copoFormsURL = '/copo/copo_forms/';
  //var copoVisualsURL = "/copo/copo_visuals/";
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = get_component_meta(component);


  //load_records(componentMeta); // call to load component records

  initialise_checklist_id();
  var args_dict = { singlecell_checklist_id: get_checklist_id() };
  $('.download-blank-manifest-template').attr(
    'href',
    $('#blank_manifest_url_' + get_checklist_id()).val()
  );
  args_dict['profile_id'] = $('#profile_id').val(),
  load_records(componentMeta, args_dict, columnDefs); // call to load component records  

  //register_resolvers_event(); //register event for publication resolvers

  //instantiate/refresh tooltips
  refresh_tool_tips();

  //trigger refresh of table
  $('body').on('refreshtable', function (event) {
    do_render_component_table_tabs(globalDataBuffer, componentMeta);
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
    .off('click', '.new-singlecell-spreadsheet-template')
    .on('click', '.new-singlecell-spreadsheet-template', function (event) {
      $('#singlecell_spreadsheet_modal #span_checklist').html(get_selected_checklist_name());
      $('#singlecell_spreadsheet_modal').modal({
        //"closable": false,
        "show": true,
        "size": BootstrapDialog.SIZE_WIDE}
      );
    });

    $('#singlecell_spreadsheet_modal').on('show.bs.modal', function (event) {
        var modal = $(this)     
        modal.find('#upload_singlecell_manifest_button').prop("disabled", false)
        modal.find('#save_singlecell_button').prop('disabled', true)   
   
        modal.find('#upload_singlecell_manifest_button')
        .off('click')
        .on('click', function (event) {
          modal.find('#file').click()
        });

        modal.find('#save_singlecell_button')
        .off('click')
        .on('click', function (event) {
          $(this).prop('disabled', true);
          modal.find('#upload_singlecell_manifest_button').prop("disabled", true)
          save_singlecell_data();          
        });
      
        modal.find('#file')
        .off('change')
        .on('change', function (event) {
          if (
            $(this).prop('files') == undefined ||
            $(this).prop('files').length == 0
          ) {
            return;
          }
          modal.find('#upload_singlecell_manifest_button').prop('disabled', true);
          modal.find('#save_singlecell_button').prop('disabled', "true")

          tabs = $('#singlecell-tabs');
          tab_content = $('#singlecell-tab-content');
          tabs.empty();
          tab_content.empty();

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

    reset_value();
    var args_dict = {};
    args_dict['singlecell_checklist_id'] = this.value;
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
  $('.new-singlecell-spreadsheet-template')
    .css('color', 'white')
    .css('background-color', colour);
 
  //******************************Functions Block******************************//

  function do_record_task(event) {
    var task = event.task.toLowerCase(); //action to be performed e.g., 'Edit', 'Delete'
    var tableID = event.tableID; //get target table
    var profile_id = $('#profile_id').val(); //get profile id

    //retrieve target records and execute task
    var table = $('#' + tableID).DataTable();
    var records = []; //
    $.map(table.rows('.selected').data(), function (item) {
      records.push(item);
    });

    var args_dict = { singlecell_checklist_id: get_checklist_id()};

    // Download sample manifest
    if (task == 'download-singlecell-manifest') {
      $('#download-singlecell-manifest-link').attr(
        'href',
        '/copo/copo_single_cell/download_manifest/' + profile_id + "/" + records[0].study_id
      );
      $('#download-singlecell-manifest-link span').trigger('click');
      return;
    }
    else {
      var study_id = '';
      for (var i = 0; i < records.length; i++) {
        if (study_id == '') {
          study_id = records[i].study_id;
        } else if (study_id != records[i].study_id) {
          study_id = '';
          break
        }
      }
      args_dict['study_id'] = study_id;
      form_generic_task('singlecell', task, records, args_dict);
    }  
  }

  $('body').on('posttablerefresh', function (event) {
    if (event.tableID == componentMeta.tableID+"_study") {
      if (current_study_id == '') {
        $("#"+ event.tableID + " tbody tr:first").addClass('selected');
        current_study_id = $("#"+ event.tableID + " tbody tr:first").attr('id');
      } else {
        $("#" + current_study_id).addClass('selected');
      }
    }
    /*
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

  $(document).on('click', "#"+ componentMeta.tableID + "_study tbody tr", function (e) {
    selected_id = $(e.currentTarget).attr("id")
    if (selected_id != current_study_id) {  
      $('#'+current_study_id).removeClass('selected');
      $(e.currentTarget).addClass('selected');
      current_study_id = selected_id
      id = selected_id.substring(selected_id.lastIndexOf("_")+1);
      load_records(componentMeta, { singlecell_checklist_id: get_checklist_id(), profile_id: $('#profile_id').val(), study_id: id }, columnDefs);
    }
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
      url: '/copo/copo_single_cell/parse_singlecell_spreadsheet/',
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
      $('#singlecell_spreadsheet_modal').find('#upload_singlecell_manifest_button').prop("disabled", false)
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
      $('#singlecell_spreadsheet_modal').find('#upload_singlecell_manifest_button').prop("disabled", false)
      $('#singlecell_spreadsheet_modal').find('#save_singlecell_button').prop("disabled", false)
    });
}

function save_singlecell_data() {
  $.ajax({
    url: '/copo/copo_single_cell/save_singlecell_records',
  })
    .fail(function (data) {
      $('#singlecell_spreadsheet_modal').find('#upload_singlecell_manifest_button').prop("disabled", false)
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
      result_dict['message'] = 'Single Cell records are saved';
      do_crud_action_feedback(result_dict);
      $('#singlecell_spreadsheet_modal').modal('hide');
      globalDataBuffer = data;

      if (data.hasOwnProperty('table_data')) {
        var event = jQuery.Event('refreshtable');
        $('body').trigger(event);
      }
    });
}
