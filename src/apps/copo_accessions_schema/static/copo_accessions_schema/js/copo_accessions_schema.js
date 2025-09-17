//uid = document.location.href;
//uid = uid.split('/');
//uid = uid[uid.length - 2];


function get_study_id() {
  if ($('#study_id').length > 0) {
    return $('#study_id').find(':selected').val();
  } else {
    return '';
  }
}

$(document).on('document_ready', function () {
  profile_id = $('#profile_id').val();
  //uid = document.location.href
  //uid = uid.split("/")
  //uid = uid[uid.length - 2]
  //******************************Event Handlers Block*************************//
  var component = $('#nav_component_name').val(); //component name
  //var copoVisualsURL = "/copo/copo_visuals/";
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = get_component_meta(component);

  //componentMeta['component'] = "accession_singlecell"; //add component name to metadata
  var args_dict = {}
  args_dict['profile_id'] = profile_id;
  args_dict['study_id'] = get_study_id();
  load_records(componentMeta, args_dict); // call to load component records

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
    form_generic_task(component, task, records);
  }

  $('#study_id').change(function () {
    if ($.fn.dataTable.isDataTable('#' + componentMeta.tableID)) {
      //if table instance already exists, then do refresh
      table = $('#' + componentMeta.tableID).DataTable();
      table.clear().destroy();
      $('#' + componentMeta.tableID).empty();
    }
    var args_dict = {};
    args_dict['study_id'] = this.value;
    args_dict['profile_id'] = $('#profile_id').val();
    load_records(componentMeta, args_dict); // call to load component records
  });


});

