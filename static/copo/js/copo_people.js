/**
 * Created by etuka on 06/11/15.
 */

$(document).ready(function () {
  //******************************Event Handlers Block*************************//
  var component = 'person';
  var copoFormsURL = '/copo/copo_forms/';
  var csrftoken = $.cookie('csrftoken');

  //get component metadata
  var componentMeta = getComponentMeta(component);

  //load records
  load_records(componentMeta);

  //instantiate/refresh tooltips
  refreshToolTips();

  //trigger refresh of table data
  $('body').on('refreshtable', function (event) {
    do_render_component_table(globalDataBuffer, componentMeta);
  });

  //handle task button event
  $('body').on('addbuttonevents', function (event) {
    do_record_task(event);
  });

  //add new component button
  $(document).on('click', '.new-component-template', function (event) {
    initiateFormCall(component);
  });

  //details button hover
  $(document).on('mouseover', '.detail-hover-message', function (event) {
    $(this).prop('title', 'Click to view ' + component + ' details');
  });

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
    if (task == 'add') {
      initiateFormCall(component);
      return false;
    }

    //edit task
    if (task == 'edit') {
      $.ajax({
        url: copoFormsURL,
        type: 'POST',
        headers: { 'X-CSRFToken': csrftoken },
        data: {
          task: 'form',
          component: component,
          target_id: records[0].record_id, //only allowing row action for edit, hence first record taken as target
        },
        success: function (data) {
          json2HtmlForm(data);
        },
        error: function () {
          alert("Couldn't build person form!");
        },
      });
    }

    //table.rows().deselect(); //deselect all rows
  }
});
