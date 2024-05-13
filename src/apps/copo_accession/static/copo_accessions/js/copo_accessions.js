let accessions_table;
let table_id = 'accessions_table';

// Store 'showAllCOPOAccessions' value
let showAllCOPOAccessions =
  $('#showAllCOPOAccessions').val() === 'True' ? true : false;

let isOtherAccessionsTabActive;
let isUserProfileActive;

if (showAllCOPOAccessions) {
  // Store data for accessions dashboard
  isOtherAccessionsTabActive = false;
  isUserProfileActive = false;
} else {
  // Store data for accessions web page for a given profile
  isUserProfileActive = true;
}

// URL for accessions dashboard where the view for it does not require user to be logged in
let copoVisualiseAccessionsURL = '/copo/copo_visualize_accessions/';
let copoVisualiseURL = '/copo/copo_visualize/';
let url = showAllCOPOAccessions ? copoVisualiseAccessionsURL : copoVisualiseURL;

let dt_options = {
  bDestroy: true,
  buttons: [
    {
      extend: 'csv',
      text: 'Export CSV',
      title: null,
      filename: `copo_${table_id}_data`,
    },
  ],
  dom: 'Bfr<"row"><"row info-rw" i>tlp',
  lengthChange: true,
  lengthMenu: [10, 25, 50, 75, 100, 500, 1000, 2000, 3000, 4000, 5000],
  ordering: true,
  processing: true,
  responsive: true,
  rowId: '_id',
  search: {
    regex: true,
    return: true,
  },
  searchHighlight: true,
  serverSide: true,
  scrollX: true,
  scrollY: 500,
  select: false,
  initComplete: function () {
    let api = this.api();
    // Add filter checkbox under  the info panel to the right side of the web page
    // Get accession types from the 'accession_type' column in the data table
    let accession_types = api.column(2).data().unique().sort().toArray();
    get_filter_accession_titles(accession_types);
  },
  createdRow: function (row, data, index) {
    // Add the record ID and accession type to each row
    let recordID = index;

    try {
      recordID = data.record_id;
      accessionType = data.accession_type;

      if (isOtherAccessionsTabActive) {
        profile_id = data.profile_id;
      }
    } catch (error) {
      console.log(`Error: ${error.message}`);
    }

    $(row).addClass(table_id + recordID);
    $(row).addClass(`${table_id}_row`);
    $(row).attr('id', recordID);
    $(row).attr('accession_type', accessionType);

    if (isOtherAccessionsTabActive) {
      // Set the profile ID as an attribute of the last cell in the row
      $(row).find('td:last-child').attr('data-profile_id', profile_id);
    }
  },
  fnRowCallback: function (nRow, aData, iDisplayIndex, iDisplayIndexFull) {
    $(nRow)
      .children()
      .each(function (index, td) {
        if (index > 0) {
          if (td.innerText === 'NA') {
            $(td).addClass('na_color');
          } else if (td.innerText === '') {
            $(td).addClass('empty_color');
          }
        }
      });
  },
  fnDrawCallback: function () {
    // Show accessions legend if it was hidden and if the table has data
    if (accessions_table.data().any()) {
      $('.accessions-legend').show();
      $('.accessions-checkboxes').show(); // Show the filter accessions legend
    }

    // Add hyperlink to all columns that have accessions
    $('.ena-accession').each(function (i, obj) {
      if ($(obj).prop('tagName') != 'TH' && $(obj).text() != '') {
        $(obj).html(
          "<a class='no-underline' href='https://www.ebi.ac.uk/ena/browser/view/" +
            $(obj).text() +
            "' target='_blank'>" +
            $(obj).text() +
            '</a>'
        );
      }
    });
    // Add hyperlink associated with COPO API
    $('.copo-api').each(function (i, obj) {
      if ($(obj).prop('tagName') != 'TH' && $(obj).text() != '') {
        $(obj).html(
          "<a class='no-underline' href='/api/manifest/" +
            $(obj).text() +
            "' target='_blank'>" +
            $(obj).text() +
            '</a>'
        );
      }
    });
  },
  ajax: {
    url: '/copo/copo_accessions/generate_accession_records',
    method: 'POST',
    headers: { 'X-CSRFToken': $.cookie('csrftoken') },
    data: function (d) {
      let checkedValues = getValues($('.filter-accessions:checked:visible'));
      let filter_accessions = JSON.stringify(checkedValues);

      return {
        isUserProfileActive: isUserProfileActive,
        isOtherAccessionsTabActive: isOtherAccessionsTabActive,
        filter_accessions: filter_accessions,
        draw: d.draw,
        order: d.order,
        length: d.length,
        start: d.start,
        search: d.search.value,
      };
    },
    dataSrc: 'data',
  },
};

$(document).ready(function () {
  const acceptRejectSampleURL = '/copo/dtol_submission/accept_reject_sample';
  const accessionsDashboardURL = '/copo/copo_accessions/dashboard';
  const tolInspectURL = '/copo/tol_dashboard/tol_inspect';
  const tolInspectByGALURL = '/copo/tol_dashboard/tol_inspect/gal';

  const sample_manager_groups = [
    'dtol_sample_managers',
    'dtolenv_sample_managers',
    'erga_sample_managers',
  ];

  const user_and_sample_manager_groups = [
    'dtol_users',
    'dtol_sample_managers',
    'dtolenv_sample_managers',
    'erga_users',
    'erga_sample_managers',
  ];

  $(document).on('click', '.accept_reject_samples', function () {
    document.location = acceptRejectSampleURL;
  });

  $(document).on('click', '.tol_inspect', function () {
    document.location = tolInspectURL;
  });

  $(document).on('click', '.tol_inspect_gal', function () {
    document.location = tolInspectByGALURL;
  });

  $(document).on('click', '.copo_accessions', function () {
    document.location = accessionsDashboardURL;
  });

  $(document).on('click', '.toggle-view', toggle_accessions_view);

  $(document).on('change', '.filter-accessions', filterRecordsByAccessionType);

  if (groups.some((x) => sample_manager_groups.indexOf(x) !== -1)) {
    $('.accept_reject_samples').show(); // Show 'accept/reject samples' button
  }

  if (groups.some((x) => user_and_sample_manager_groups.indexOf(x) !== -1)) {
    $('.tol_inspect').show(); // Show 'tol_inspect' button
    $('.tol_inspect_gal').show(); // Show 'tol_inspect_gal' button
  }

  // Hide buttons if user does not belong to a group
  if (groups.length === 0) {
    $('.tol_inspect').hide(); // Hide 'tol_inspect' button
    $('.tol_inspect_gal').hide(); // Hide 'tol_inspect_gal' button
  }

  // Hide the accessions' button
  // from the Accessions dashboard web page
  if (showAllCOPOAccessions) {
    $('.copo_accessions').hide();
  }

  // Load records
  load_accessions_records();

  // Instantiate/refresh tooltips
  refresh_tool_tips();
}); //End document ready

//______________Handlers___________________________________

// Get a list of values from a jQuery object
const getValues = function ($el) {
  let items = [];
  $el.each(function () {
    items.push($(this).val());
  });

  return items;
};

function filterRecordsByAccessionType() {
  // Get the checked accession types
  let checkedAccessions = getValues($('.filter-accessions:checked:visible'));

  // Separate each accession by a pipe symbol
  let accession_types = checkedAccessions.join('|');

  // Get records based on the checked accession type(s)
  accessions_table.column(2).search(accession_types, true, false).draw();
}

function get_filter_accession_titles(accession_types) {
  $.ajax({
    url: '/copo/copo_accessions/get_filter_accession_titles',
    method: 'POST',
    headers: { 'X-CSRFToken': $.cookie('csrftoken') },
    dataType: 'json',
    data: {
      isOtherAccessionsTabActive: isOtherAccessionsTabActive,
      accession_types: JSON.stringify(accession_types),
    },
    success: function (data) {
      if (data.length === 0) {
        return false;
      } else {
        set_filter_checkboxes(data);
      }
    },
    error: function (error) {
      console.log(`Error: ${error.message}`);
    },
  });
}

function set_filter_checkboxes(accession_titles) {
  let accessions_checkboxes = $('.accessions-checkboxes');

  // Clear accessions checkboxes div if data exists within it
  if (accessions_checkboxes.length) accessions_checkboxes.empty();

  $.each(accession_titles, function (index, item) {
    let label = isOtherAccessionsTabActive ? item.title : item.value;

    let $filterCheckBoxItem = '<div class="form-check">';
    $filterCheckBoxItem +=
      '<input id="' +
      item.value +
      '" ' +
      'class="filter-accessions form-check-input" ' +
      'type="checkbox" value="' +
      item.value +
      '"/>';
    $filterCheckBoxItem +=
      '<label class="form-check-label" style="padding-left: 5px" for="' +
      item.value +
      '">';
    $filterCheckBoxItem += label;
    $filterCheckBoxItem +=
      '<i class="fa fa-info-circle accession_type_info_icon" title="' +
      item.title +
      '"> </i>';
    $filterCheckBoxItem += '</label>';
    $filterCheckBoxItem += '</div>';
    $filterCheckBoxItem += '<br/>';

    // Click the first accession type shown in the 'filter accessions types' legend
    // Usually, "Project" accession type filter checkbox will be
    // checked in the 'Other Accessions' tab
    // if (isOtherAccessionsTabActive && index === 0) {
    //   $filterCheckBoxItem = $filterCheckBoxItem.replace(
    //     'type="checkbox"',
    //     'type="checkbox" checked'
    //   );
    // }

    // Check all checkboxes by default in the 'Sample Accessions' tab
    // if (!isOtherAccessionsTabActive) {
    //   $filterCheckBoxItem = $filterCheckBoxItem.replace(
    //     'type="checkbox"',
    //     'type="checkbox" checked'
    //   );
    // }

    // Populate the accessions checkboxes div with the accession types checkboxes
    $('.accessions-legend')
      .find('.accessions-checkboxes')
      .append($filterCheckBoxItem);
  });
}

function place_accessions_task_buttons(componentMeta) {
  //place custom buttons on table
  if (!componentMeta.recordActions.length) {
    return;
  }

  var table = $('#' + componentMeta.tableID).DataTable();

  var customButtons = $('<span/>', {
    style: 'padding-left: 15px;',
    class: 'copo-table-cbuttons',
  });

  $(table.buttons().container()).append(customButtons);

  componentMeta.recordActions.forEach(function (item) {
    var actionBTN = $('.record-action-templates')
      .find('.' + item)
      .clone();
    actionBTN.removeClass(item);
    actionBTN.attr('data-table', componentMeta.tableID);
    customButtons.append(actionBTN);
  });

  refresh_tool_tips();
}

function customise_accessions_table(table) {
  const componentMeta = get_component_meta($('#nav_component_name').val());
  const tableID = `#${componentMeta.tableID}`;

  // Add buttons to the table
  table
    .buttons()
    .nodes()
    .each(function (value) {
      $(this).removeClass('btn btn-default').addClass('tiny ui basic button');
    });

  // Show task buttons on accessions dashboard and
  // on the accessions web page for a given profile
  place_accessions_task_buttons(componentMeta);

  let table_wrapper = $(tableID + '_wrapper');

  table_wrapper
    .find('.dataTables_filter')
    .find('label')
    .css({ padding: '20px 0 20px 0', 'margin-top': '10px' })
    .find('input')
    .removeClass('input-sm')
    .attr('placeholder', 'Search ' + componentMeta.title)
    .attr('size', 30);

  // Add css to align the buttons to the right
  table_wrapper.find('.dt-buttons').addClass('pull-right');
  table_wrapper.find('.info-rw').hide(); // Hide showing 'x' of 'x' row

  // Insert breakpoints after the toggle button
  if (table_wrapper.find('br').length === 0) {
    $('<br><br>').insertAfter(table_wrapper.find('.dt-buttons'));
  }

  // Set height of table to fit the content in the table
  table_wrapper.find('.dataTables_scrollBody').css('height', 'fit-content');

  // Add padding between table and show records filter
  table_wrapper.find('.dataTables_length').css('padding-top', '20px');
}

function load_accessions_records() {
  let accessions_checkboxes = $('.accessions-checkboxes');

  let order = isOtherAccessionsTabActive ? [[4, 'desc']] : [[3, 'desc']];

  let columnDefinition = [
    {
      targets: '_all', // all fields
      createdCell: function (td, cellData, rowData, row, col) {
        if (typeof cellData == 'undefined') {
          $(td).addClass('cell-no-content');
          $(td).text('');
        }
      },
    },
    {
      targets: '_all', // all fields
      defaultContent: '',
    },
  ];

  $.ajax({
    url: '/copo/copo_accessions/get_accession_records_column_names',
    data: {
      isUserProfileActive: isUserProfileActive,
      isOtherAccessionsTabActive: isOtherAccessionsTabActive,
    },
    method: 'GET',
    dataType: 'json',
  })
    .fail(function (data) {
      console.log('Error: ' + data);
    })
    .done(function (data) {
      if (data.length) {
        // if table instance already exists then, clear, destroy and empty the table
        if ($.fn.DataTable.isDataTable(`#${table_id}`)) {
          $(`#${table_id}`).DataTable().clear().destroy();
          $(`#${table_id}`).empty();

          // Set toggle button on accessions dashboard and
          // on the accessions web page for a given profile
          set_toggle_button();
        }
        dt_options['columns'] = data;
        dt_options['columnDefs'] = columnDefinition;
        dt_options['order'] = order;

        accessions_table = $(`#${table_id}`)
          .DataTable(dt_options)
          .columns.adjust()
          .draw();

        // Add buttons and other things to the table
        customise_accessions_table(accessions_table);
      } else {
        // Hide accessions legend if table is empty
        accessions_checkboxes.empty();

        // Hide the 'filter accession types' legend
        $('.accessions-legend').hide();
      }
    });
}

function toggle_accessions_view() {
  let accessions_checkboxes = $('.accessions-checkboxes');
  $(this).find('.btn').toggleClass('active');

  if ($(this).find('.btn-success').length > 0) {
    $(this).find('.btn').toggleClass('btn-success');
  }

  $(this).find('.btn').toggleClass('btn-default');

  if ($(this).find('.active').text().includes('Other')) {
    isOtherAccessionsTabActive = true;
  } else {
    isOtherAccessionsTabActive = false;
  }
  accessions_checkboxes.empty();
  load_accessions_records();
}

function set_toggle_button() {
  $('.toggle-view').find('.btn').toggleClass('active');
  $('.toggle-view').find('.btn').toggleClass('btn-success');
  $('.toggle-view').find('.btn').toggleClass('btn-default');
}
