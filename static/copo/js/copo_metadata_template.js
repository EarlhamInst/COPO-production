$(document).ready(function () {
  get_wizard_types();
  //$(document).on("click", "#export_button", function () {
  //    $("#export_modal").modal("show")
  //})
  $(document).on('click', '#add_fields_button', add_primer_fields);
  // load existing terms
  $.getJSON(
    '/copo/load_metadata_template_terms/',
    { template_id: $('#template_id').val() },
    load_terms_from_backend
  );

  $('#template_content').sortable({
    stop: update_template,
  });
  $(document).on('click', '.delete_button', function (event) {
    $(event.currentTarget).closest('.annotation_term').remove();
    update_template(event, this);
  });
  $(document).on('click', '.update_title_button', function (event) {
    $('.new_title_div').addClass('loading');
    var title = $('#new_title').val();
    var template_id = $('#template_id').val();
    if (title) {
      $.ajax({
        url: '/copo/update_metadata_template_name/',
        data: { template_name: title, template_id: template_id },
        type: 'GET',
      }).done(function (data) {
        $('#new_title').val('');
        $('#new_title').attr('placeholder', data);
        $('.new_title_div').removeClass('loading');
      });
    } else {
      $('.new_title_div').removeClass('loading');
    }
  });
  $(document).on(
    'click',
    '#template_dd_button',
    template_dd_button_click_handler
  );
  //******************************Event Handlers Block*************************//
  var component = 'metadata_template';
  var copoFormsURL = '/copo/copo_forms/';
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = getComponentMeta(component);
  var args_dict = {};
  args_dict['profile_id'] = $('#profile_id').val();
  load_records(componentMeta, args_dict); // call to load component records

  //register_resolvers_event(); //register event for publication resolvers

  //instantiate/refresh tooltips
  refreshToolTips();

  //trigger refresh of table
  $('body').on('refreshtable', function (event) {
    do_render_component_table(globalDataBuffer, componentMeta);
  });

  //handle task button event
  $('body').on('addbuttonevents', function (event) {
    do_record_task(event);
  });

  //add new component button
  //add new component button
  $(document).on('click', '.new-component-template', function (event) {
    event.task = 'add';
    do_record_task(event);
  });

  //details button hover
  $(document).on('mouseover', '.detail-hover-message', function (event) {
    $(this).prop('title', 'Click to view ' + component + ' details');
  });

  //******************************Functions Block******************************//

  function do_record_task(event) {
    var task = event.task.toLowerCase(); //action to be performed e.g., 'Edit', 'Delete'
    // var tableID = event.tableID; //get target table
    //
    // //retrieve target records and execute task
    // var table = $('#' + tableID).DataTable();
    // var records = []; //
    // $.map(table.rows('.selected').data(), function (item) {
    //     records.push(item);
    // });
    //add task
    if (task == 'add') {
      BootstrapDialog.show({
        title: 'Create New Template',
        message:
          '<form id="template_name_form" role="form" data-toggle="validator">' +
          '<div class="form-group">\n' +
          '<label for="template_name">Template Name</label>\n' +
          '<input type="text" class="form-control" id="template_name" placeholder="" data-error="Template Name is Required" required>\n' +
          '<div class="help-block with-errors"></div>\n' +
          '</div>\n' +
          '</form>',
        onshow: function (dialog) {},
        buttons: [
          {
            label: 'Cancel',
            hotkey: 27, // Keycode of esc
            action: function (dialogRef) {
              dialogRef.close();
            },
          },
          {
            label: 'Create',
            hotkey: 13,
            action: function (dialogRef) {
              $('#template_name_form').validator('validate');
              var template_name = $(dialogRef.$modalContent)
                .find('#template_name')
                .val();
              if (template_name) {
                $.ajax({
                  url: '/copo/new_metadata_template/',
                  data: { template_name: template_name },
                  type: 'GET',
                }).done(function (data) {
                  dialogRef.close();
                  window.location = data;
                });
              }
            },
          },
        ],
      });
    }
    if (task == 'delete') {
      alert('implement delete');
    }
    //edit task
    if (task == 'edit') {
      window.location =
        '/copo/author_template/' + records[0].record_id + '/view';
    }
    //table.rows().deselect(); //deselect all rows
  }

  $('#template_content').droppable({
    drop: function (event, ui) {
      var d = ui.draggable[0];
      $(d).css('width', '50%').css('margin', '30px 0 0 30px');
      $(this).append(ui.draggable[0]);
      $(ui.draggable[0]).draggable({ disabled: 'true' });
      $(ui.draggable[0]).find('.delete_button').show();
    },
    //activeClass: "dropActive",
    //tolerance: "pointer",
    // drop: metadata_drop_handler,
    //accept:".annotation_term"
  });

  function metadata_drop_handler(ev, ui) {
    var data = new Object();
    var iri = $(ui.draggable.context).data('iri');
    data.label = $(ui.draggable.context).data('label');
    data.id = $(ui.draggable.context).data('id');
    data.obo_id = $(ui.draggable.context).data('obo_id');
    data.ontology_name = $(ui.draggable.context).data('ontology_name');
    data.ontology_prefix = $(ui.draggable.context).data('ontology_prefix');
    data.short_form = $(ui.draggable.context).data('short_form');
    data.type = $(ui.draggable.context).data('type');
    data.description = $(ui.draggable.context).data('description');
  }
}); //end document ready

function update_template(event, ui) {
  // get elements in sortable
  $('.loading_div').show();
  var items = $('#template_content').children();
  var terms = new Array();
  $(items).each(function (idx, el) {
    $(el).find('.count').html();
    $(el)
      .find('.count')
      .html(idx + 1);
    $(el).data('order', idx);
    $(el).removeData('ui-draggable');
    $(el).removeData('sortableItem');
    $(el).removeData('sortable-item');
    terms.push($(el).data());
  });
  var template_id = $('#template_id').val();

  terms = JSON.stringify(terms);
  csrftoken = $.cookie('csrftoken');
  $.ajax({
    url: '/copo/update_template/',
    data: { data: terms, template_id: template_id },
    type: 'POST',
    headers: {
      'X-CSRFToken': csrftoken,
    },
  }).done(function (data) {});
  $('.loading_div').hide();
}

$(document).on('click', '#export_button', create);

//setTimeout("create('Hello world!', 'myfile.txt', 'text/plain')");
function create(evt) {
  evt.preventDefault();
  var dlbtn = document.getElementById('export_save_btn');

  $('.loading_div').show();
  var template_id = $('#template_id').val();
  csrftoken = $.cookie('csrftoken');
  var xhr = new XMLHttpRequest();
  xhr.open('POST', '/copo/export_template/');
  xhr.onreadystatechange = function () {
    if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
      let link = document.createElement('a');
      let blob = new Blob([this.response], {});
      link.download = 'export.csv';
      link.href = URL.createObjectURL(blob);
      link.click();
      window.URL.revokeObjectURL(link.href);
      $('.loading_div').hide();
    }
  };
  xhr.setRequestHeader('X-CSRFToken', csrftoken);
  xhr.responseType = 'blob';
  xhr.send(JSON.stringify({ template_id: template_id }));
}

function get_wizard_types() {
  $.ajax({
    url: '/copo/get_wizard_types/',
    type: 'GET',
    dataType: 'json',
  }).done(function (data) {
    $('#template_dd').empty();
    $(data).each(function (idx, element) {
      var option =
        "<option value='" + element.value + "'>" + element.key + '</option>';
      $('#template_dd').append(option);
    });
    $(document).data(
      'primer_template',
      $('#template_dd').find(':selected').val()
    );
  });
}

function template_dd_button_click_handler() {
  let filename = $('#template_dd').find(':selected').val();
  $(document).data('primer_template', filename);
  $.ajax({
    url: '/copo/get_primer_fields/',
    type: 'GET',
    dataType: 'json',
    data: { filename: filename },
  }).done(function (data) {
    $('#field_primer_modal').find('#field_primer_form').empty();

    $(data).each(function (idx, el) {
      var items = el.items;
      $(el.items).each(function (id, ell) {
        let checked = '';
        console.log(ell);
        if (ell.required == 'true') {
          checked = 'checked="checked" disabled="disabled"';
        }
        var d = '<div class="form-check">';
        d =
          d +
          '<input class="form-check-input" type="checkbox" ' +
          checked +
          ' value="' +
          ell.id +
          '" id="' +
          ell.id +
          '">\n' +
          '<label class="form-check-label" for="' +
          ell.id +
          '">\n' +
          ell.label +
          '\n' +
          '  </label>';
        d = d + '</div>';
        $('#field_primer_modal').find('#field_primer_form').append(d);
      });
    });

    $('#field_primer_modal').modal('show');
  });
}

function add_primer_fields() {
  let fields = $('#field_primer_form')
    .find(':checked')
    .map(function () {
      return this.id;
    })
    .get()
    .join();
  let filename = $(document).data('primer_template');
  csrftoken = $.cookie('csrftoken');
  $.ajax({
    url: '/copo/add_primer_fields/',
    type: 'POST',
    dataType: 'json',
    data: {
      fields: fields,
      filename: filename,
      template_id: $('#template_id').val(),
    },
    headers: {
      'X-CSRFToken': csrftoken,
    },
  }).done(function (data) {
    load_terms_from_backend(data);

    $('#field_primer_modal').modal('hide');
  });
}

function load_terms_from_backend(data) {
  $(data.terms).each(function (idx, entry) {
    var result = $('<div/>', {
      class: 'annotation_term panel panel-default align-middle',
      'data-iri': entry.iri,
      'data-label': entry.label,
      'data-id': entry.id,
      'data-obo_id': entry.obo_id,
      'data-ontology_name': entry.ontology_name,
      'data-ontology_prefix': entry.ontology_prefix,
      'data-short_form': entry.short_form,
      'data-type': entry.type,
      'data-is_search_result': true,
    });

    $(result).append(
      $('<span/>', {
        html:
          entry.label + ' - <small>' + entry['ontology_prefix'] + '</small>',
        class: '',
        style: '',
      })
    );
    $(result).append(
      '<span style="display: none; margin-left: 10px" class="ui red compact icon button delete_button pull-right">\n' +
        '  <i class="trash icon"></i>\n' +
        '</span>'
    );

    $(result).append(
      "<span style='font-size: x-large' class='count pull-right'></span>"
    );
    $(result).css('width', '50%').css('margin', '30px 0 0 30px');
    $(result).find('.delete_button').show();
    $(result)
      .find('.count')
      .html(entry.order + 1);
    //$(result).append("<span style='font-size: x-large' class='count pull-right'>" + (entry.order + 1) + "</span>")
    $('#template_content').append(result);
  });
}
