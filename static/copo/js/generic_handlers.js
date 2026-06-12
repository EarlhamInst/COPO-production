/**
 * generic_handlers.js
 *
 * Core utility and event-handler module for the COPO Django web application.
 *
 * This file is loaded on every COPO page and provides:
 *   - Global document.ready bootstrap (navigation, autocomplete, select events)
 *   - Select2 / Selectize widget initialisation and refresh helpers
 *   - Ontology lookup, popover display, and icon state management
 *   - Component record view and delete confirmation dialogs
 *   - DataTables rendering and column-definition helpers
 *   - Form-control refresh functions (datepicker, validator, range slider,
 *     selectbox, multiselect, ontology, general lookup, etc.)
 *   - Autocomplete integration with the OLS (Ontology Lookup Service)
 *   - Data-display panel builders (collapsible lists, attribute tables)
 *   - Component navigation, icon management, and profile-type routing
 *   - WebUI popover and context-help system
 *   - Dialog and UI template factories (panels, alerts, menus)
 *   - Modal/dialog utilities (close confirmation, value reset, info migration)
 *
 * Depends on: jQuery, Select2, Selectize, DataTables, BootstrapDialog,
 *             WebuiPopovers, component_def, profile_type_def, title_button_def
 */

// ═══ GLOBALS ══════════════════════════════════════════════════════════════════

var AnnotationEventAdded = false;
var selectizeObjects = {}; //stores reference to selectize objects initialised on the page
var copoVisualsURL = '/copo/copo_visualize/';
var csrftoken = $.cookie('csrftoken');

// ═══ DOCUMENT READY — BOOTSTRAP ═══════════════════════════════════════════════

$(document).ready(function () {
  var componentName = $('#nav_component_name').val();

  setup_autocomplete();

  //set up global navigation components if component is available
  if (componentName) {
    do_page_controls(componentName);
  }

  var timeout;
  var delay = 1000;

  // Debounce keyboard navigation in Selectize dropdowns
  $(document).on('keyup', function (event) {
    if (timeout) {
      clearTimeout(timeout);
    }
    timeout = setTimeout(function () {
      set_selectize_select_event(event);
    }, delay);
  });

  ontology_link_event();

  select2_mouse_event();

  select2_data_view_event();

  setup_dismissable_message();

  setup_collapsible_event();

  setup_copo_general_lookup_event();

  initialiseNavToggle();

  var event = jQuery.Event('document_ready'); // individual components can trap and handle this event
  $(document).trigger(event);
});

// ═══ UI EVENT HANDLERS ════════════════════════════════════════════════════════

/**
 * Binds Bootstrap collapse show/hide events to toggle the plus/minus icon
 * on `.copo-details-coll` panel headings.
 */
function setup_collapsible_event() {
  $(document)
    .on('show.bs.collapse', '.copo-details-coll.collapse', function (event) {
      $(this)
        .prev('.panel-heading')
        .find('.fa')
        .removeClass('fa-plus')
        .addClass('fa-minus');
    })
    .on('hide.bs.collapse', '.copo-details-coll.collapse', function () {
      $(this)
        .prev('.panel-heading')
        .find('.fa')
        .removeClass('fa-minus')
        .addClass('fa-plus');
    });
}

/**
 * Binds a click event to `.copo-tag` elements that toggles the visibility of
 * the associated `.copo-tag-content` panel via a slow slide animation.
 */
function setup_copo_general_lookup_event() {
  $(document).on('click', '.copo-tag ', function (event) {
    var content = $(this).closest('.copo-item').find('.copo-tag-content');
    content.slideToggle('slow');
  });
}

/**
 * Initialises the ontology autocomplete control for annotation fields.
 * Lazily adds the OLS search URL on first focus of an annotator field and
 * ensures the global autocomplete instance is (re-)initialised.
 */
function setup_autocomplete() {
  var copoFormsURL = '/copo/copo_forms/';
  $(document).on('focus', 'input[id^="annotator-field"]', function (e) {
    t = e.currentTarget;
    $('.annotator-listing').find('ul').empty();
    $(t).addClass('ontology-field');

    if (!AnnotationEventAdded) {
      $(t).attr('data-autocomplete', '/copo/ajax_search_ontology/999/');

      auto_complete();
      AnnotationEventAdded = true;
    }
  });
  auto_complete();
}

/**
 * Binds a delegated click handler so that clicking the close button inside a
 * `.message` element removes the entire message from the DOM.
 */
function setup_dismissable_message() {
  $(document).on('click', '.message .close', function () {
    $(this).closest('.message').remove();
  });
}

/**
 * Builds and returns a Semantic UI success message element suitable for use
 * as an inline feedback pane.
 *
 * @returns {jQuery} A jQuery-wrapped message div with header, body, and close icon.
 */
function get_feedback_pane() {
  return $(
    '<div class="ui success message" style="margin-bottom: 10px;">\n' +
      '                        <i class="close icon"></i>\n' +
      '                        <div class="header"></div>\n' +
      '                        <p></p>\n' +
      '                        <div class="m-body"></div>\n' +
      '                    </div>'
  );
}

// ═══ SELECT2 / SELECTIZE EVENT HANDLERS ══════════════════════════════════════

/**
 * Binds a delegated click handler to `.copo-embedded` elements.
 * On click, fetches a description for the embedded item from the server and
 * displays it in a sticky webuiPopover attached to the clicked element.
 */
function select2_data_view_event() {
  $(document).on('click', '.copo-embedded', function () {
    var item = $(this);
    lookupsURL = $('#ajax_search_copo_local').val();
    var localolsURL = lookupsURL.replace('999', item.data('source'));
    var accession = item.data('accession');

    item.find('.fa').addClass('fa-spin');
    item.addClass('text-primary');
    item.webuiPopover('destroy');

    $.ajax({
      url: localolsURL,
      type: 'GET',
      headers: {
        'X-CSRFToken': csrftoken,
      },
      data: {
        accession: accession,
      },
      success: function (data) {
        if (data.hasOwnProperty('result') && data.result.length > 0) {
          var desc = data.result[0].description;
          WebuiPopovers.updateContent(
            item,
            '<div class="webpop-content-div limit-text">' + desc + '</div>'
          );

          item.webuiPopover({
            content:
              '<div class="webpop-content-div limit-text">' + desc + '</div>',
            trigger: 'sticky',
            width: 300,
            arrow: true,
            placement: 'right',
            dismissible: true,
            closeable: true,
          });

          item.removeClass('text-primary');
          item.find('.fa').removeClass('fa-spin');
        }
      },
      error: function () {
        item.removeClass('text-primary');
        item.find('.fa').removeClass('fa-spin');
        item.webuiPopover('destroy');
        alert("Couldn't retrieve item's details!");
      },
    });
  });
}

/**
 * Binds a delegated mouseover handler to `.server-desc` elements inside
 * Select2 dropdown options. On hover, fetches the item description from the
 * server and updates the webuiPopover content in place.
 */
function select2_mouse_event() {
  $(document).on('mouseover', '.server-desc', function () {
    var item = $(this);

    var parentElem = item.closest('.parentSpan');
    var url = parentElem.attr('data-url');
    var accession = parentElem.attr('data-id');

    if (!(url && accession)) {
      return false;
    }

    WebuiPopovers.updateContent(
      item,
      '<div class="webpop-content-div"><span class="fa fa-spinner fa-pulse fa-2x"></span></div>'
    );

    $.ajax({
      url: url,
      type: 'GET',
      headers: {
        'X-CSRFToken': csrftoken,
      },
      data: {
        accession: accession,
      },
      success: function (data) {
        if (data.hasOwnProperty('result') && data.result.length > 0) {
          var desc = data.result[0].description;
          WebuiPopovers.updateContent(
            item,
            '<div class="webpop-content-div limit-text">' + desc + '</div>'
          );
        }
      },
      error: function () {
        console.log("Couldn't retrieve item's details!");
      },
    });
  });
}

/**
 * Binds a keyup handler on `.ontology-field` inputs so that when the user
 * edits a previously resolved ontology value, all associated hidden fields
 * within the same `.ontology-parent` are cleared, preventing stale data.
 */
function ontology_value_change() {
  $(document).on('keyup', '.ontology-field', function () {
    var elem = $(this);
    elem
      .closest('.ontology-parent')
      .find('.ontology-field-hidden')
      .each(function () {
        $(this).val('');
      });
  });
}

/**
 * Binds a delegated click handler to `.non-free-text` elements that toggles
 * the visibility of the extra ontology info panel (`.onto-label-more`)
 * associated with the clicked label.
 */
function ontology_link_event() {
  $(document).on('click', '.non-free-text', function () {
    $(this).closest('.onto-label').find('.onto-label-more').toggle();
  });
}

/**
 * Handles keyboard up/down navigation within Selectize dropdown controls.
 * When the active item changes, fetches and displays a contextual popover
 * with details about the highlighted option.  Supports four control types:
 * onto-select (ontology), general-onto (generic object), copo-multi-search
 * (cross-component record), and copo-lookup (server-side lookup).
 *
 * @param {jQuery.Event} event - The keyup event from the delegated document handler.
 */
function set_selectize_select_event(event) {
  var keyCode = event.keyCode || event.which;
  if (keyCode === 38 || keyCode === 40) {
    if ($(event.target).closest('.onto-select').length) {
      var item = $(event.target).closest('.onto-select');
      var activeElem = item
        .find('.selectize-dropdown-content .active')
        .find('.onto-label');

      var desc = activeElem.attr('data-desc');
      var prefix = activeElem.attr('data-prefix');
      var label = activeElem.attr('data-label');
      var accession = activeElem.attr('data-accession');

      showontopop(item, label, prefix, desc, accession);
    } else if ($(event.target).closest('.general-onto').length) {
      var item = $(event.target).closest('.general-onto');
      var activeElem = item
        .find('.selectize-dropdown-content .active')
        .find('.onto-label');

      var indx = activeElem.data('indx');
      var parentid = activeElem.data('parentid');
      var lblField = activeElem.data('lblfield');
      var elemFields = JSON.parse(
        activeElem.closest('.ontology-parent').find('.elem-fields').val()
      );

      try {
        var valueObject = selectizeObjects[parentid].options[indx];
        showgeneraldetails(item, valueObject, elemFields, lblField);
      } catch (e) {
        console.log(
          "Couldn't retrieve control value object [value, parent id]: [" +
            indx +
            ',' +
            parentid +
            ']'
        );
      }
    } else if ($(event.target).closest('.copo-multi-search').length) {
      var eventTarget = $(event.target).closest('.copo-multi-search');
      var item = eventTarget.find('.selectize-dropdown-content .active');
      var recordId = item.attr('data-value');
      var associatedComponent = item
        .find('.caption-component')
        .attr('data-component');
      var popTarget = item.closest('.copo-form-group');

      if (associatedComponent) {
        resolve_element_view(recordId, associatedComponent, popTarget);
      }
    } else if ($(event.target).closest('.copo-lookup').length) {
      var item = $(event.target).closest('.copo-lookup');
      var activeElem = item
        .find('.selectize-dropdown-content .active')
        .find('.lookup-label');
      var desc = activeElem.attr('data-desc');
      var label = activeElem.attr('data-label');
      var accession = activeElem.attr('data-accession');
      var url = activeElem.attr('data-url');
      var serverSide = activeElem.attr('data-serverside');

      showlkup(item, label, desc, accession, url, serverSide);
    }
  }
}

$(document).on(
  'mouseenter',
  '.selectize-dropdown-content .active',
  function (event) {
    if ($(this).closest('.selectize-control.onto-select').length) {
      var item = $(this).closest('.selectize-control.onto-select');

      var desc = $(this).find('.onto-label').attr('data-desc');
      var prefix = $(this).find('.onto-label').attr('data-prefix');
      var label = $(this).find('.onto-label').attr('data-label');
      var accession = $(this).find('.onto-label').attr('data-accession');

      showontopop(item, label, prefix, desc, accession);
    } else if ($(this).closest('.selectize-control.general-onto').length) {
      var item = $(this).closest('.selectize-control.general-onto');
      var activeElem = $(this).find('.onto-label');

      var indx = $(this).find('.onto-label').data('indx');
      var parentid = activeElem.data('parentid');
      var lblField = activeElem.data('lblfield');
      var elemFields = JSON.parse(
        activeElem.closest('.ontology-parent').find('.elem-fields').val()
      );

      try {
        var valueObject = selectizeObjects[parentid].options[indx];
        showgeneraldetails(item, valueObject, elemFields, lblField);
      } catch (e) {
        console.log(
          "Couldn't retrieve control value object [value, parent id]: [" +
            indx +
            ',' +
            parentid +
            ']'
        );
      }
    } else if ($(this).closest('.selectize-control.copo-multi-search').length) {
      var item = $(this).closest('.selectize-control.copo-multi-search');

      var recordId = item
        .find('.selectize-dropdown-content .active')
        .attr('data-value');
      var associatedComponent = item
        .find('.caption-component')
        .attr('data-component');
      var popTarget = item.closest('.copo-form-group');

      if (associatedComponent) {
        resolve_element_view(recordId, associatedComponent, popTarget);
      }
    } else if ($(this).closest('.selectize-control.copo-lookup').length) {
      var item = $(this).closest('.selectize-control.copo-lookup');

      var desc = $(this).find('.lookup-label').attr('data-desc');
      var label = $(this).find('.lookup-label').attr('data-label');
      var accession = $(this).find('.lookup-label').attr('data-accession');

      var url = $(this).find('.lookup-label').attr('data-url');
      var serverSide = $(this).find('.lookup-label').attr('data-serverside');

      showlkup(item, label, desc, accession, url, serverSide);
    }
  }
);

// ═══ ONTOLOGY LOOKUP / POPOVER DISPLAY ════════════════════════════════════════

/**
 * Displays a webuiPopover containing lookup details (label, accession, description)
 * for a Selectize copo-lookup item.  If the item is flagged as server-side, the
 * description is fetched via AJAX before the popover is rendered.
 *
 * @param {jQuery}  item       - The Selectize control element to attach the popover to.
 * @param {string}  label      - Display label for the selected term.
 * @param {string}  desc       - Description text (may be resolved server-side).
 * @param {string}  accession  - Accession/ID of the selected term.
 * @param {string}  url        - Ajax endpoint used when serverSide is truthy.
 * @param {boolean} serverSide - Whether to resolve the description from the server.
 */
function showlkup(item, label, desc, accession, url, serverSide) {
  var show_lkup_details = function () {
    var result = $('<div/>', {
      class: 'limit-text',
    });

    item.webuiPopover('destroy');

    if (String(accession) != 'undefined' && String(label) != 'undefined') {
      //ontology accession
      var lookupAccession = $('<div/>');

      if (accession != '') {
        lookupAccession.css('margin-top', '5px');
        $('<span>', {
          class: 'ontology-accession-link',
          html:
            "<span style='text-decoration-line: underline; color:#2759a5'>" +
            accession +
            '</span>',
        }).appendTo(lookupAccession);
      }

      result.append(lookupAccession);

      //ontology description
      var lookupDescription = $('<div/>');

      if (desc != '') {
        lookupDescription.css('margin-top', '5px');
        lookupDescription.html(desc);
      }

      result.append(lookupDescription);

      item.webuiPopover({
        title: label,
        content:
          '<div class="webpop-content-div">' +
          $('<div/>').append(result).html() +
          '</div>',
        trigger: 'sticky',
        width: 300,
        arrow: true,
        placement: 'right',
        dismissible: true,
      });
    }
  };

  if (serverSide) {
    //resolve item description from the server
    $.ajax({
      url: url,
      type: 'POST',
      headers: {
        'X-CSRFToken': csrftoken,
      },
      data: {
        accession: accession,
      },
      success: function (data) {
        if (data.hasOwnProperty('result') && data.result.length > 0) {
          desc = data.result[0].description;
          show_lkup_details();
        }
      },
      error: function () {
        console.log("Couldn't retrieve item's details!");
      },
    });
  } else {
    show_lkup_details();
  }
}

/**
 * Displays a webuiPopover showing ontology source, accession link, and
 * description for a highlighted item in an onto-select Selectize control.
 *
 * @param {jQuery} item      - The Selectize control element to attach the popover to.
 * @param {string} label     - Human-readable label for the ontology term.
 * @param {string} prefix    - Ontology source prefix (e.g. "EFO"); empty string for free-text values.
 * @param {string} desc      - Description of the ontology term.
 * @param {string} accession - IRI or accession identifier for the term.
 */
function showontopop(item, label, prefix, desc, accession) {
  var result = $('<div/>', {
    class: 'limit-text',
  });

  item.webuiPopover('destroy');

  if (String(accession) != 'undefined' && String(label) != 'undefined') {
    //ontology source
    var ontologySource = $('<div/>', {
      html: 'This is a free-text value',
    });

    if (prefix != '') {
      ontologySource.html('Ontology source: ' + prefix);
    }

    result.append(ontologySource);

    //ontology accession
    var ontologyAccession = $('<div/>');

    if (accession != '') {
      ontologyAccession.css('margin-top', '5px');
      $('<span>', {
        class: 'ontology-accession-link',
        html:
          "<span style='text-decoration-line: underline; color:#2759a5'>" +
          accession +
          '</span>',
      }).appendTo(ontologyAccession);
    }

    result.append(ontologyAccession);

    //ontology description
    var ontologyDescription = $('<div/>');

    if (desc != '') {
      ontologyDescription.css('margin-top', '5px');
      ontologyDescription.html(desc);
    }

    result.append(ontologyDescription);

    item.webuiPopover({
      title: label,
      content:
        '<div class="webpop-content-div">' +
        $('<div/>').append(result).html() +
        '</div>',
      trigger: 'sticky',
      width: 300,
      arrow: true,
      placement: 'right',
      dismissible: true,
    });
  }
}

/**
 * Displays a webuiPopover summarising the fields of a general-ontology
 * (non-OLS) lookup item, rendering only schema fields marked `show_in_table`.
 *
 * @param {jQuery}   item       - The Selectize control element to attach the popover to.
 * @param {Object}   dataObject - The raw value object from the Selectize options store.
 * @param {Object[]} schema     - Field schema array; each entry has `id`, `label`, and `show_in_table`.
 * @param {string}   lblField   - Key within `dataObject` to use as the popover title.
 */
function showgeneraldetails(item, dataObject, schema, lblField) {
  var result = $('<div/>', {
    class: 'limit-text',
  });

  item.webuiPopover('destroy');

  let message = $('<div class="webpop-content-div"></div>');
  var codeList = $('<div class="ui relaxed divided list"></div>');

  result.append(message);
  message.append(codeList);

  for (var i = 0; i < schema.length; ++i) {
    var schemaNode = schema[i];
    if (
      schemaNode.hasOwnProperty('show_in_table') &&
      schemaNode.show_in_table.toString().toLowerCase() == 'true'
    ) {
      var itemNode = $('<div class="item"></div>');
      codeList.append(itemNode);
      itemNode.append(
        '<div class="content"><div class="header">' +
          schemaNode.label +
          '</div><div class="description webpop-content-div">' +
          dataObject[schemaNode.id] +
          '</div></div>'
      );
    }
  }

  item.webuiPopover({
    title: dataObject[lblField],
    content:
      '<div class="webpop-content-div">' +
      $('<div/>').append(result).html() +
      '</div>',
    trigger: 'sticky',
    width: 300,
    arrow: true,
    placement: 'right',
    dismissible: true,
  });
}

// ═══ COMPONENT RECORD VIEW / DELETE ═══════════════════════════════════════════

/**
 * Fetches display attributes for a record from the visualise endpoint and
 * shows them in a webuiPopover attached to `eventTarget`.
 * Maps a form element's selected value (record ID) to a component type
 * (e.g. source, sample) for attribute rendering.
 *
 * @param {string} recordId            - The MongoDB ObjectId of the target record.
 * @param {string} associatedComponent - Component type string (e.g. "source").
 * @param {jQuery} eventTarget         - The element to which the popover is attached.
 */
function resolve_element_view(recordId, associatedComponent, eventTarget) {

  if (associatedComponent == '') {
    return false;
  }

  $.ajax({
    url: copoVisualsURL,
    type: 'POST',
    headers: {
      'X-CSRFToken': csrftoken,
    },
    data: {
      task: 'attributes_display',
      component: associatedComponent,
      target_id: recordId,
    },
    success: function (data) {
      var title = 'Attributes';
      gAttrib = build_attributes_display(data);

      if (data.component_label) {
        title = data.component_label;
      }

      eventTarget.webuiPopover('destroy');

      eventTarget.webuiPopover({
        title: title,
        content:
          '<div class="webpop-content-div limit-text">' +
          $('<div/>').append(gAttrib).html() +
          '</div>',
        trigger: 'sticky',
        width: 300,
        arrow: true,
        placement: 'right',
        dismissible: true,
      });
    },
    error: function () {
      var message = "Couldn't retrieve attributes!";

      eventTarget.webuiPopover('destroy');

      eventTarget.webuiPopover({
        title: 'Info',
        content: '<div class="webpop-content-div">' + message + '</div>',
        trigger: 'sticky',
        width: 300,
        arrow: true,
        placement: 'right',
        dismissible: true,
      });
    },
  });
}

/**
 * Shows a BootstrapDialog danger confirmation prompt before deleting records.
 * On confirmation, POSTs a delete request and re-renders the DataTable via
 * `do_render_table`.
 *
 * @param {Object}   params            - Delete parameters.
 * @param {string[]} params.target_ids - Array of record IDs to delete.
 * @param {string}   params.component  - Component type string (e.g. "sample").
 */
function do_component_delete_confirmation(params) {
  var targetComponentBody =
    'Please confirm delete action for the selected records.';
  var targetComponentTitle = 'Delete Alert!';

  var doTidyClose = {
    closeIt: function (dialogRef) {
      dialogRef.close();
    },
  };

  var code = BootstrapDialog.show({
    type: BootstrapDialog.TYPE_DANGER,
    title: $('<span>' + targetComponentTitle + '</span>'),
    message: function () {
      var $message = $('<span>' + targetComponentBody + '</span>');
      return $message;
    },
    draggable: true,
    closable: true,
    animate: true,
    onhide: function () {},
    buttons: [
      {
        label: 'Cancel',
        action: function (dialogRef) {
          doTidyClose['closeIt'](dialogRef);
        },
      },
      {
        icon: 'glyphicon glyphicon-trash',
        label: 'Delete',
        cssClass: 'btn-danger',
        action: function (dialogRef) {
          csrftoken = $.cookie('csrftoken');
          var target_ids = JSON.stringify(params.target_ids);

          $.ajax({
            url: copoFormsURL,
            type: 'POST',
            headers: {
              'X-CSRFToken': csrftoken,
            },
            data: {
              task: 'delete',
              component: params.component,
              target_ids: target_ids,
            },
            success: function (data) {
              do_render_table(data);
            },
            error: function () {
              alert("Couldn't delete records!");
            },
          });

          doTidyClose['closeIt'](dialogRef);
        },
      },
    ],
  });
}

// ═══ DATATABLES RENDERING ═════════════════════════════════════════════════════

/**
 * Initialises or refreshes a DataTable from a server response payload.
 *
 * Behaviour:
 * - If the table already exists and `data.table_data.row_data` is set, a
 *   single new row is appended and highlighted.
 * - If the table already exists without row_data, all rows are replaced
 *   (e.g. after a delete).
 * - If the table does not yet exist, a full DataTable is constructed with
 *   column definitions, global action buttons, and a hook event
 *   (`addbuttonevents`) for per-table button wiring.
 * - For the `datafile_table` specifically, an `addtoqueue` event is fired
 *   so the upload queue can track the new record.
 *
 * @param {Object} data                            - Server response object.
 * @param {Object} data.table_data                 - Table configuration payload.
 * @param {string} data.table_data.table_id        - DOM id of the target table element.
 * @param {Array}  data.table_data.dataSet         - Full row dataset for initialisation/refresh.
 * @param {Array}  data.table_data.row_data        - Single-row data for append mode.
 * @param {Array}  data.table_data.columns         - DataTables column definitions.
 * @param {Object} data.table_data.action_buttons  - Button config with `global_btns` and `row_btns`.
 */
function do_render_table(data) {
  var table = null;
  var lastRecord = null;
  var filterDivObject = null;
  var lengthDivObject = null;

  if ($.fn.dataTable.isDataTable('#' + data.table_data.table_id)) {
    //if table instance already exists, then this is probably a refresh after an CRUD operation
    table = $('#' + data.table_data.table_id).DataTable();
  }

  if (data.table_data.row_data) {
    //adding single record to table

    if (table) {
      //this test is probably redundant, but...you never can tell!
      //remove previously highlighted row
      table
        .rows('.row-insert-higlight')
        .nodes()
        .to$()
        .removeClass('row-insert-higlight');

      var rowNode = table.row.add(data.table_data.row_data).draw().node();

      //highlight new row
      $(rowNode).addClass('row-insert-higlight').animate({
        color: 'black',
      });

      lastRecord = data.table_data.row_data;
    }
  } else {
    //probably first time rendering table, or maybe a call to refresh whole table, say after a delete action

    if (table) {
      //definitely a call to refresh table, as table instance exists!
      table.clear().draw();
      table.rows.add(data.table_data.dataSet); // Add new data
      table.columns.adjust().draw();
      return;
    }

    //get default ordering index;
    //ideally we would want to order by record creation date.
    //if it exists, it should be the last but one item in the columns list
    var orderIndx = 0;
    if (data.table_data.columns.length > 2) {
      orderIndx = data.table_data.columns.length - 2;
    }

    //custom column rendering
    var colDefs = [];

    //button coldefs
    var btnDef = {
      targets: -1,
      data: null,
      searchable: false,
      orderable: false,
      render: function (rdata) {
        var rndHTML = '';
        if (data.table_data.action_buttons.row_btns) {
          var bTns = data.table_data.action_buttons.row_btns; //row buttons
          rndHTML = '<span style="white-space: nowrap;">';
          for (var i = 0; i < bTns.length; ++i) {
            rndHTML +=
              '<a data-action-target="row" data-record-action="' +
              bTns[i].btnAction +
              '" data-record-id="' +
              rdata[rdata.length - 1] +
              '" data-toggle="tooltip" data-container="body" style="display: inline-block; white-space: normal;" title="' +
              bTns[i].text +
              '" class="' +
              bTns[i].className +
              ' btn-xs"><i class="' +
              bTns[i].iconClass +
              '"> </i><span></span></a>&nbsp;';
          }
          rndHTML += '</span>';
        }
        return rndHTML;
      },
    };

    colDefs.push(btnDef);

    //determine how columns are rendered - cols definition
    var custDef;
    var v = [];
    for (var i = 0; i < data.table_data.columns.length - 1; ++i) {
      v.push(i);
    }

    //exception for datafile table, it treats the first column differently
    if (data.table_data.table_id == 'datafile_table') {
      v.splice(0, 1);
    }

    custDef = {
      targets: v,
      data: data,
      render: function (data, type, row, meta) {
        var rndHTML = '';
        if (
          Object.prototype.toString.call(data[meta.col]) === '[object String]'
        ) {
          rndHTML = data[meta.col];
        } else if (typeof data[meta.col] === 'object') {
          var collapseLink = data[data.length - 1] + '_' + meta.col;
          rndHTML = get_data_item_collapse(
            collapseLink,
            get_data_list_panel(data[meta.col], collapseLink),
            data[meta.col].length
          );
        }

        return rndHTML;
      },
    };

    colDefs.push(custDef);

    //end cols definition

    //column def for datafile table
    if (data.table_data.table_id == 'datafile_table') {
      colDefs.push({
        targets: 0,
        data: null,
        render: function (rdata) {
          var containerRow = $('<div/>', {
            class: 'row',
            style: 'margin-left:-10px; white-space: nowrap; margin-right: 5px;',
          });

          var metadataDiv = $('<div></div>').attr({
            class: 'col-sm-1 col-md-1 col-lg-1 itemMetadata-flag',
            'data-record-id': rdata[rdata.length - 1],
            style: 'cursor: hand; cursor: pointer; display: inline-block;',
          });

          var spanPoor = $('<span/>', {
            class: 'itemMetadata-flag-ind meta-active poor',
            style: 'margin-top: 3px;',
          });

          var spanFair = $('<span/>', {
            class: 'itemMetadata-flag-ind fair',
          });

          var spanGood = $('<span/>', {
            class: 'itemMetadata-flag-ind good',
          });

          metadataDiv.append(spanPoor).append(spanFair).append(spanGood);

          var dataDiv = $('<div></div>').attr({
            class: 'col-sm-11 col-md-11 col-lg-11',
            style:
              'margin-left: -10px; margin-top: 10px; display: inline-block;',
          });

          var dataSpan = $('<span/>', {
            html: rdata[0],
          });

          var descFlagSpan = $('<i></i>').attr({
            class: 'fa fa-tags inDescription-flag',
            'data-record-id': rdata[rdata.length - 1],
            'data-toggle': 'tooltip',
            style: 'padding-left: 5px; display: none;',
            title: 'Currently being described',
          });

          dataDiv.append(dataSpan).append(descFlagSpan);
          containerRow.append(metadataDiv).append(dataDiv);

          return $('<div></div>').append(containerRow).html();
        },
      });
    }

    var scrollX = true;

    if (data.table_data.table_id == 'datafile_table') {
      //scroll has undesirable effect on this table
      scrollX = false;
    }

    table = $('#' + data.table_data.table_id).DataTable({
      data: data.table_data.dataSet,
      columns: data.table_data.columns,

      paging: true,
      ordering: true,
      scrollX: scrollX,
      lengthChange: true,
      order: [[orderIndx, 'desc']],
      select: {
        style: 'multi',
      },
      dom: 'lf<"row button-rw">rtip',
      fnDrawCallback: function (oSettings) {
        refresh_tool_tips();

        $('.dataTables_filter').each(function () {
          if ($(this).attr('id') == data.table_data.table_id + '_filter') {
            filterDivObject = $(this);
            return false;
          }
        });

        $('.dataTables_length').each(function () {
          if ($(this).attr('id') == data.table_data.table_id + '_length') {
            lengthDivObject = $(this);
            return false;
          }
        });

        //trigger metadata refresh for datafiles
        if (data.table_data.table_id == 'datafile_table') {
          var event = jQuery.Event('refreshmetadataevents');
          $('body').trigger(event);
        }
      },
      columnDefs: colDefs,
    });

    //attach global action buttons
    if (data.table_data.action_buttons.global_btns) {
      var custBtns = data.table_data.action_buttons.global_btns; //global buttons
      var actBtns = ['selectAll', 'selectNone'];
      var bTns = custBtns.concat(actBtns);
      new $.fn.dataTable.Buttons(table, {
        buttons: bTns,
      });

      table
        .buttons()
        .nodes()
        .each(function (value) {
          $(this).addClass(' btn-sm');
        });

      table
        .buttons('.copo-dt')
        .nodes()
        .each(function (value) {
          var btnImage;
          for (var i = 0; i < bTns.length; ++i) {
            if (bTns[i].text == this.text) {
              btnImage = bTns[i];
              break;
            }
          }

          $(this).removeClass('btn-default'); //remove default class
          $(this).addClass(this.className); //attach supplied class
          $(this).attr('data-record-action', btnImage.btnAction); //data attribute to signal action type
          $(this).attr('data-action-target', 'rows'); //data attribute to signal batch action
          $(this).attr(
            'data-tour-id',
            data.table_data.table_id + '_' + btnImage.btnAction
          ); //quick tour component: table_id + action type
          //
          ////attach icon to button
          try {
            $('<i class="' + bTns[value].iconClass + '">&nbsp;</i>').prependTo(
              $(this)
            );
          } catch (err) {}
        });

      $('div.button-rw').append(filterDivObject);
      $('div.button-rw').append(lengthDivObject);
      $('div.button-rw').append($(table.buttons().container()));
      $(lengthDivObject).addClass('pad-it');
    }

    //create a hook for attaching button events in individual table handlers
    if ($.fn.dataTable.isDataTable('#' + data.table_data.table_id)) {
      var tableID = data.table_data.table_id;
      var event = jQuery.Event('addbuttonevents');
      event.tableID = tableID;
      $('body').trigger(event);
    }
  }

  //handle requests for specific tables...
  if (data.table_data.table_id == 'datafile_table') {
    table = $('#' + data.table_data.table_id).DataTable();
    if (table && lastRecord) {
      //trigger event for queuing record
      var event = jQuery.Event('addtoqueue');
      event.recordLabel = lastRecord[0];
      event.recordID = lastRecord[lastRecord.length - 1];
      $('body').trigger(event);
    }
  }
} //end of function

// ═══ FORM CONTROL REFRESH FUNCTIONS ═══════════════════════════════════════════

/**
 * Re-initialises all dynamic form controls on the page.
 * Called after DataTable draws to ensure controls rendered inside table cells
 * are properly bootstrapped.  Covers tooltips, popovers, Semantic UI dropdowns,
 * color overrides, every Selectize/Select2 variant, range sliders, autocomplete,
 * and the datepicker.
 */
function refresh_tool_tips() {
  $("[data-toggle='tooltip']").tooltip();
  $("[data-toggle='popover']").popover();
  $('.ui.dropdown').dropdown();
  $('.copo-tooltip').popup();

  apply_color();
  refresh_selectbox();
  refresh_select2box();
  refresh_multiselectbox();
  refresh_multiselect2box();
  refresh_singleselectbox();
  refresh_multisearch();
  refresh_ontology_select();
  refresh_general_ontology_search();
  refresh_general_ontology_select();
  refresh_copo_lookup();
  refresh_copo_lookup2();

  refresh_range_slider();
  auto_complete();

  setup_datepicker();
} //end of func

/**
 * Initialises Bootstrap datepicker on all `.date-picker` inputs.
 * The date format differs between DToL sample pages (yyyy-mm-dd) and
 * standard ENA pages (dd/mm/yyyy).
 */
function setup_datepicker() {
  var format_string;
  // dtol date format and ENA date formats are sadly different, so check if we are dealing with a dtol sample
  if ($(document).data('isDtolSamplePage')) {
    format_string = 'yyyy-mm-dd';
  } else {
    format_string = 'dd/mm/yyyy';
  }
  $('.date-picker').datepicker({
    format: format_string,
  });
}

/**
 * Triggers a validator update on the given form object so that any newly
 * rendered fields are included in subsequent validation passes.
 *
 * @param {jQuery} formObject - The jQuery-wrapped form element.
 */
function refresh_validator(formObject) {
  formObject.validator('update');
}

/**
 * Initialises the rangeslider.js polyfill on all `.range-slider` inputs.
 * Updates the paired output element and hidden value element on slide.
 */
function refresh_range_slider() {
  $('.range-slider').each(function () {
    var elem = $(this);

    var outputElem = elem
      .closest('.range-slider-parent')
      .find('.range-slider-output');
    var elemValue = elem.closest('.range-slider-parent').find('.elem-value');

    elem.rangeslider({
      // Feature detection the default is `true`.
      // Set this to `false` if you want to use
      // the polyfill also in Browsers which support
      // the native <input type="range"> element.
      polyfill: false,

      // Default CSS classes
      rangeClass: 'rangeslider',
      disabledClass: 'rangeslider--disabled',
      horizontalClass: 'rangeslider--horizontal',
      verticalClass: 'rangeslider--vertical',
      fillClass: 'rangeslider__fill',
      handleClass: 'rangeslider__handle',

      // Callback function
      onInit: function () {},

      // Callback function
      onSlide: function (position, value) {
        outputElem.html(value);
      },

      // Callback function
      onSlideEnd: function (position, value) {
        outputElem.html(value);
        elemValue.val(value);
      },
    });
  });
} //end of function

/**
 * Initialises Selectize on all `.copo-select` elements that have not already
 * been instantiated.  Allows free-text entry and displays a remove button.
 */
function refresh_selectbox() {
  $('.copo-select').each(function () {
    var elem = $(this);

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      elem.selectize({
        delimiter: ',',
        plugins: ['remove_button'],
        persist: false,
        create: function (input) {
          return {
            value: input,
            text: input,
          };
        },
      });
    }
  });
} //end of function

/**
 * Initialises Select2 (with tags enabled) on all `.copo-select2` elements
 * that have not already been initialised.
 */
function refresh_select2box() {
  $('.copo-select2').each(function () {
    var elem = $(this);

    if (!elem.hasClass('select2-hidden-accessible')) {
      elem.select2({
        tags: true,
        data: JSON.parse(elem.attr('data-currentValue')),
        // dropdownParent: $(this).closest(".copo-form-group")
      });
    }
  });
} //end of function

/**
 * Initialises Select2 on all `.copo-multi-select2` elements that have not
 * already been initialised, restoring any pre-selected values.
 */
function refresh_multiselect2box() {
  $('.copo-multi-select2').each(function () {
    var elem = $(this);

    if (!elem.hasClass('select2-hidden-accessible')) {
      elem.select2({
        data: JSON.parse(elem.attr('data-optionsList')),
        maximumSelectionLength: elem.attr('data-maximumSelectionLength'),
        // dropdownParent: $(this).closest(".copo-form-group")
      });

      elem.val(JSON.parse(elem.attr('data-currentValue')));
      elem.trigger('change');
    }
  });
} //end of function

/**
 * Initialises Select2 on all `.copo-single-select` elements that have not
 * already been initialised.  Renders a webuiPopover info icon next to options
 * that carry a description field.
 */
function refresh_singleselectbox() {
  $('.copo-single-select').each(function () {
    var elem = $(this);

    if (!elem.hasClass('select2-hidden-accessible')) {
      elem.select2({
        data: JSON.parse(elem.attr('data-optionsList')),
        dropdownParent: $(this).closest('.copo-form-group'),
        escapeMarkup: function (markup) {
          return markup;
        }, // let our custom formatter work
        templateResult: function (state) {
          if (!state.id) {
            return state.text;
          }

          var descr = state.description || '';
          var $state = $('<span>' + state.text + '</span>');

          if (descr) {
            var item = $(
              '<i class="ui grey icon info circle" style="margin-left: 15px;"></i>'
            );
            item.webuiPopover('destroy');
            item.webuiPopover({
              content: '<div class="webpop-content-div">' + descr + '</div>',
              arrow: true,
              width: 200,
              trigger: 'hover',
            });
            $state.append(item);
          }

          return $state;
        },
      });

      elem.val(JSON.parse(elem.attr('data-currentValue')));
      elem.trigger('change');
    }
  });
} //end of function

/**
 * Initialises Select2 AJAX lookup on all `.copo-lookup2` elements that have
 * not already been initialised.  Fetches options dynamically from the endpoint
 * specified by the element's `data-url` attribute, using the current profile ID
 * as an additional filter parameter.
 */
function refresh_copo_lookup2() {
  var profile_id = '';
  if ($('#profile_id').length) {
    profile_id = $('#profile_id').val();
  }

  $('.copo-lookup2').each(function () {
    var elem = $(this);

    if (!elem.hasClass('select2-hidden-accessible')) {
      elem.select2({
        maximumSelectionLength: elem.attr('data-maximumSelectionLength'),
        data: JSON.parse(elem.attr('data-currentValue')),
        dropdownParent: $(this).closest('.copo-form-group'),
        ajax: {
          url: elem.attr('data-url'),
          dataType: 'json',
          type: 'GET',
          headers: {
            'X-CSRFToken': csrftoken,
          },
          delay: 250,
          data: function (params) {
            return {
              q: params.term, // search term
              profile_id: profile_id,
              referenced_field: elem.attr('data-ref'),
            };
          },
          processResults: function (data) {
            var res = data.result.map(function (item) {
              var serverSide = false;
              if (item.hasOwnProperty('server-side')) {
                serverSide = item['server-side'];
              }
              return {
                id: item.accession,
                text: item.label,
                serverSide: serverSide,
                url: elem.attr('data-url'),
              };
            });
            return {
              results: res,
            };
          },
          cache: true,
        },
        minimumInputLength: 1,
        escapeMarkup: function (markup) {
          return markup;
        }, // for our custom formatter to work
        templateResult: function (state) {
          var $state = $(
            '<span  data-id="' +
              state.id +
              '" data-server="' +
              state.serverSide +
              '" data-url="' +
              state.url +
              '" class="parentSpan">' +
              state.text +
              '</span>'
          );

          var item = $(
            '<i class="ui grey icon info server-desc circle" style="margin-left: 15px;"></i>'
          );

          item.webuiPopover('destroy');
          item.webuiPopover({
            content: '<div class="webpop-content-div"></div>',
            arrow: true,
            width: 200,
            trigger: 'hover',
          });

          $state.append(item);
          return $state;
        },
      });

      selectizeObjects[elem.attr('id')] = elem;
    }
  });
} //end of function

/**
 * Initialises Selectize on all `.copo-multi-select` elements that have not
 * already been instantiated.  Syncs the selected value back to a hidden
 * `.copo-multi-values` element on change, and retains a reference in
 * `selectizeObjects`.
 */
function refresh_multiselectbox() {
  $('.copo-multi-select').each(function () {
    var elem = $(this);
    var valueElem = elem.closest('.ctrlDIV').find('.copo-multi-values');
    var maxTems = 'null'; //maximum selectable items

    var parentID = valueElem.attr('id');

    if (valueElem.is('[data-maxItems]')) {
      maxTems = valueElem.attr('data-maxItems');
    }

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var $funSelect = elem.selectize({
        onChange: function (value) {
          if (value) {
            valueElem.val(value).trigger('change');
          } else {
            valueElem.val('');
          }
        },
        //dropdownParent: 'body',
        maxItems: maxTems,
        plugins: ['remove_button'],
      });

      //set default values
      var control = $funSelect[0].selectize;
      control.setValue(valueElem.val().split(',')); //set default value

      //retain reference to control for any future reference
      selectizeObjects[parentID] = control;
    }
  });
}

/**
 * Initialises Selectize on all `.copo-lookup` elements that have not already
 * been instantiated.  Performs server-side search via the element's `data-url`
 * attribute, syncs selected accession to a hidden `.copo-multi-values` element,
 * and retains a reference in `selectizeObjects`.
 */
function refresh_copo_lookup() {
  $('.copo-lookup').each(function () {
    var elem = $(this);
    var url = elem.attr('data-url');
    var valueElem = elem.closest('.ctrlDIV').find('.copo-multi-values');
    var elemSpecs = JSON.parse(
      elem.closest('.ctrlDIV').find('.elem-json').val()
    );

    var maxTems = 'null'; //maximum selectable items
    if (valueElem.is('[data-maxItems]')) {
      maxTems = valueElem.attr('data-maxItems');
    }

    var options = [];

    var profile_id = '';
    if ($('#profile_id').length) {
      profile_id = $('#profile_id').val();
    }

    if (elemSpecs.length > 0) {
      elemSpecs.forEach(function (item) {
        if (item.accession == valueElem.val()) options.push(item);
      });
    }

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var $funSelect = elem.selectize({
        onChange: function (value) {
          if (value) {
            valueElem.val(value).trigger('change');
          } else {
            valueElem.val('');
          }

          WebuiPopovers.hideAll();
        },
        onBlur: function () {
          WebuiPopovers.hideAll();
        },
        maxItems: maxTems,
        create: false,
        plugins: ['remove_button'],
        valueField: 'accession',
        labelField: 'label',
        searchField: 'label',
        options: options,
        render: {
          option: function (item, escape) {
            var desc = escape(item.description);
            var accession = escape(item.accession);
            var label = escape(item.label);
            var serverSide = false;
            if (item.hasOwnProperty('server-side')) {
              serverSide = item['server-side'];
            }

            return (
              '<div>' +
              '<span  data-serverside="' +
              serverSide +
              '" data-url="' +
              url +
              '" data-accession="' +
              accession +
              '"  data-label="' +
              label +
              '" data-desc="' +
              desc +
              '" class="webpop-content-div ontology-label lookup-label">' +
              escape(item.label) +
              '</span></div>'
            );
          },
        },
        load: function (query, callback) {
          if (!query.length) return callback();
          this.clearOptions(); // clear the data
          this.renderCache = {}; // clear the html template cache
          $.ajax({
            url: url,
            type: 'POST',
            headers: {
              'X-CSRFToken': csrftoken,
            },
            data: {
              q: query,
              profile_id: profile_id,
            },
            success: function (data) {
              var items = [];

              if (data.hasOwnProperty('result') && data.result.length > 0) {
                items = data.result;
              }

              callback(items);
            },
            error: function () {
              callback();
            },
          });
        },
      });

      //reference to selectize object
      var control = $funSelect[0].selectize;

      //set default values
      if (options.length) {
        for (var p in control.options) {
          control.setValue(p); //set default value
        }
      }

      //retain reference to control for any future reference
      selectizeObjects[valueElem.attr('id')] = control;
    }
  });
}

/**
 * Initialises Selectize on all `.onto-select` elements that have not already
 * been instantiated.  Handles OLS term search via AJAX, maps the selected term
 * object to hidden `annotationValue / termSource / termAccession / comments`
 * fields, updates the display label, and drives the ontology icon state.
 */
function refresh_ontology_select() {
  $('.onto-select').each(function () {
    var elem = $(this);
    var url = elem.attr('data-url');
    var options = [];
    var defaultValue = {};

    var parentID = elem
      .closest('.ontology-parent')
      .find('.ontology-field-hidden')
      .attr('id');

    //set previous value
    elem
      .closest('.ontology-parent')
      .find('.ontology-field-hidden')
      .each(function () {
        defaultValue[$(this).attr('data-key')] = $(this).val();
      });

    if (
      defaultValue.hasOwnProperty('annotationValue') &&
      defaultValue.annotationValue.trim() != ''
    ) {
      var option = {};
      option.labelblank = defaultValue.annotationValue;
      if (option.labelblank.length > 9) {
        option.labelblank = option.labelblank.substr(0, 9) + '...';
      }
      option.label = defaultValue.annotationValue;
      option.ontology_prefix = defaultValue.termSource;
      option.iri = defaultValue.termAccession;
      option.description = defaultValue.comments;
      option.copo_values = JSON.stringify({
        termAccession: defaultValue.termAccession,
        termSource: defaultValue.termSource,
        annotationValue: defaultValue.annotationValue,
        comments: defaultValue.comments,
      });

      options.push(option);
    }

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var $funSelect = elem.selectize({
        onChange: function (value) {
          if (value) {
            try {
              value = JSON.parse(value);
              //set value
              var setValue = '';

              if (typeof value === 'object') {
                elem
                  .closest('.ontology-parent')
                  .find('.ontology-field-hidden')
                  .each(function () {
                    var dataKey = $(this).attr('data-key');
                    $(this).val(value[dataKey]);
                  });
                setValue = value.annotationValue;
              } else {
                // a string slipped through - apparently numbers can be parsed to JSON
                elem
                  .closest('.ontology-parent')
                  .find('.ontology-field-hidden')
                  .each(function () {
                    var dataKey = $(this).attr('data-key');

                    $(this).val('');

                    if (dataKey == 'annotationValue') {
                      $(this).val(value);
                      setValue = value;
                    }
                  });
              }

              //set display
              elem
                .closest('.ontology-parent')
                .find('.onto-label')
                .find('span.onto-label-span')
                .html(setValue);
            } catch (e) {
              //likely an unresolved or free-text entry - set only annotationValue, all others to empty
              elem
                .closest('.ontology-parent')
                .find('.ontology-field-hidden')
                .each(function () {
                  var dataKey = $(this).attr('data-key');

                  $(this).val('');

                  if (dataKey == 'annotationValue') {
                    $(this).val(value);
                  }
                });

              //set display
              elem
                .closest('.ontology-parent')
                .find('.onto-label')
                .find('span.onto-label-span')
                .html(value);
            }
          } else {
            //unset values
            elem
              .closest('.ontology-parent')
              .find('.ontology-field-hidden')
              .each(function () {
                $(this).val('');
              });

            //unset display
            elem
              .closest('.ontology-parent')
              .find('.onto-label')
              .find('span.onto-label-span')
              .html('');
          }

          WebuiPopovers.hideAll();
          set_ontology_icon(elem, $funSelect[0].selectize.getValue());
        },
        onBlur: function () {
          WebuiPopovers.hideAll();
        },
        maxItems: '1',
        create: true,
        plugins: ['remove_button'],
        valueField: 'copo_values',
        labelField: 'labelblank',
        searchField: 'label',
        options: options,
        render: {
          option: function (item, escape) {
            var desc =
              escape(item.description) != 'undefined'
                ? escape(item.description)
                : ' ';
            var prefix =
              escape(item.ontology_prefix) != 'undefined'
                ? escape(item.ontology_prefix)
                : '';
            var accession = escape(item.iri);
            var label = escape(item.label);

            return (
              '<div>' +
              '<span data-accession="' +
              accession +
              '"  data-label="' +
              label +
              '"data-prefix="' +
              prefix +
              '" data-desc="' +
              desc +
              '" class="webpop-content-div ontology-label onto-label">' +
              prefix +
              (prefix ? ': ' : '') +
              escape(item.label) +
              '</span></div>'
            );
          },
        },
        load: function (query, callback) {
          if (!query.length) return callback();
          this.clearOptions(); // clear the data
          this.renderCache = {}; // clear the html template cache
          $.ajax({
            url: url,
            type: 'GET',
            dataType: 'json',
            data: {
              q: query,
            },
            error: function () {
              callback();
            },
            success: function (data) {
              var ontologies = [];

              data.response.docs.forEach(function (item) {
                item.copo_values = JSON.stringify({
                  termAccession: item.iri,
                  termSource: item.ontology_prefix,
                  annotationValue: item.label,
                  comments: item.description,
                });

                item.labelblank = item.label;

                if (item.labelblank.length > 9) {
                  item.labelblank = item.labelblank.substr(0, 9) + '...';
                }

                ontologies.push(item);
              });
              console.log(ontologies[0]);
              callback(ontologies);
            },
          });
        },
      });

      //reference to selectize object
      var control = $funSelect[0].selectize;

      //set default values
      if (options.length) {
        for (var p in control.options) {
          control.setValue(p); //set default value
        }
      }

      set_ontology_icon(elem, control.getValue());

      //retain reference to control for any future reference
      selectizeObjects[parentID] = control;
    }
  });
}

/**
 * Initialises Selectize on all `.general-onto-search` elements that have not
 * already been instantiated.  Supports a remote data source (server-side search).
 * On selection, dynamically inserts hidden input fields for each schema property
 * and fires a custom jQuery event (`data-eventname`) for external handlers.
 */
function refresh_general_ontology_search() {
  $('.general-onto-search').each(function () {
    var elem = $(this);
    var url = elem.attr('data-url');
    var elemId = elem.attr('data-element');
    var eventName = elem.attr('data-eventname');
    var api_schema = JSON.parse(
      elem.closest('.copo-form-group').find('.elem-fields').val()
    );
    var call_params = JSON.parse(
      elem.closest('.copo-form-group').find('.elem-params').val()
    );

    var options = [];

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var $funSelect = elem.selectize({
        onChange: function (selectedValue) {
          //set value in hidden fields

          var ontologySpan = elem.closest('.ontology-parent');
          ontologySpan.find('.ontology-field-hidden').remove();
          var valueObject = {};

          if (selectedValue) {
            var schema = JSON.parse(ontologySpan.find('.elem-fields').val());

            try {
              valueObject = $funSelect[0].selectize.options[selectedValue];
              for (var i = 0; i < schema.length; ++i) {
                var fv = schema[i];
                ontologySpan.append(
                  $('<input/>', {
                    type: 'hidden',
                    class: 'ontology-field-hidden',
                    id: elemId + '.' + fv.id,
                    name: elemId + '.' + fv.id,
                    value: valueObject[fv.id],
                  })
                );
              }
            } catch (e) {
              console.log(
                "Couldn't retrieve control value object [value, parent id]: [" +
                  indx +
                  ',' +
                  parentid +
                  ']'
              );
            }
          }

          WebuiPopovers.hideAll();
          set_general_ontology_detail(elem, valueObject);

          //trigger event to be handled externally
          if (eventName) {
            let controlEvent = jQuery.Event(eventName);
            controlEvent.elementId = elemId;
            controlEvent.selectedValue = selectedValue;
            $('body').trigger(controlEvent);
          }
        },
        onBlur: function () {
          WebuiPopovers.hideAll();
        },
        score: function () {
          return function () {
            return 1; //this enables all options to be listed on upon search return
          };
        },
        maxItems: '1',
        create: false,
        plugins: ['remove_button'],
        valueField: 'copo_idblank',
        labelField: 'copo_labelblank',
        searchField: 'copo_labelblank',
        options: options,
        sortField: [{ field: 'copo_labelblank', direction: 'asc' }],
        render: {
          option: function (item, escape) {
            return (
              '<div>' +
              '<span data-lblfield="copo_labelblank" data-parentid="' +
              elemId +
              '"  data-indx="' +
              escape(item.copo_idblank) +
              '"  class="webpop-content-div ontology-label onto-label">' +
              escape(item.copo_labelblank) +
              '</span></div>'
            );
          },
          item: function (item, escape) {
            var lbl = escape(item.copo_labelblank);
            if (lbl.length > 9) {
              lbl = escape(item.copo_labelblank).substr(0, 9) + '...';
            }
            return '<div><span>' + lbl + '</span></div>';
          },
        },
        load: function (query, callback) {
          this.clearOptions(); // clear the data
          this.close();
          WebuiPopovers.hideAll();
          this.renderCache = {};
          query = query.trim();

          if (!query.length) {
            report_call_feedback(elem, 'No query term entered', 'success');
            return callback();
          }

          var params = { q: query };
          if (call_params) {
            params = $.extend(params, call_params);
          }
          params.api_schema = JSON.stringify(api_schema);
          $.ajax({
            url: url,
            type: 'GET',
            dataType: 'json',
            data: params,
            error: function () {
              callback();
            },
            success: function (data) {
              var ontologies = [];

              if (data.hasOwnProperty('message')) {
                report_call_feedback(elem, data.message, data.status);
              }

              if (data.hasOwnProperty('api_schema')) {
                //backened is sending a schema, retain this
                elem
                  .closest('.ontology-parent')
                  .find('.elem-fields')
                  .val(JSON.stringify(data.api_schema));
              }

              if (data.hasOwnProperty('status') && data.status == 'success') {
                ontologies = data.items;
              }

              callback(ontologies);
            },
          });
        },
      });

      //reference to selectize object
      selectizeObjects[elemId] = $funSelect[0].selectize;
    }
  });
}

/**
 * Initialises Selectize on all `.general-onto-select` elements that have not
 * already been instantiated.  Works on a predefined static options list only
 * (no remote data source or dynamic option updates).  Sets value from
 * `data-currentValue` and reports the option count as feedback.
 */
function refresh_general_ontology_select() {
  $('.general-onto-select').each(function () {
    var elem = $(this);
    var elemId = elem.attr('data-element');
    var eventName = elem.attr('data-eventname');
    var options = JSON.parse(
      elem.closest('.copo-form-group').find('.elem-options').val()
    );

    var idField = 'copo_idblank';
    var labelField = 'copo_labelblank';

    if (elem.attr('data-idField')) {
      idField = elem.attr('data-idField');
    }

    if (elem.attr('data-labelField')) {
      labelField = elem.attr('data-labelField');
    }

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var $funSelect = elem.selectize({
        onChange: function (selectedValue) {
          //set value in hidden fields

          var ontologySpan = elem.closest('.ontology-parent');
          ontologySpan.find('.ontology-field-hidden').remove();
          var valueObject = {};

          var valElem = document.getElementById(elemId);
          if (selectedValue) {
            valElem.value = selectedValue;

            valueObject = $funSelect[0].selectize.options[selectedValue];
          } else {
            valElem.value = '';
          }

          WebuiPopovers.hideAll();
          set_general_ontology_detail(elem, valueObject);

          //trigger event to be handled externally
          if (eventName) {
            let controlEvent = jQuery.Event(eventName);
            controlEvent.elementId = elemId;
            controlEvent.selectedValue = selectedValue;
            $(document).trigger(controlEvent);
          }
        },
        onBlur: function () {
          WebuiPopovers.hideAll();
        },
        score: function () {
          return function () {
            return 1; //this enables all options to be listed on upon search return
          };
        },
        maxItems: '1',
        create: false,
        plugins: ['remove_button'],
        valueField: idField,
        labelField: labelField,
        searchField: labelField,
        options: options,
        sortField: [{ field: labelField, direction: 'asc' }],
        render: {
          option: function (item, escape) {
            return (
              '<div>' +
              '<span data-lblfield="' +
              labelField +
              '"  data-parentid="' +
              elemId +
              '"  data-indx="' +
              escape(item[idField]) +
              '"  class="webpop-content-div ontology-label onto-label">' +
              escape(item[labelField]) +
              '</span></div>'
            );
          },
          item: function (item, escape) {
            var lbl = escape(item[labelField]);
            if (lbl.length > 9) {
              lbl = escape(item[labelField]).substr(0, 9) + '...';
            }
            return '<div><span>' + lbl + '</span></div>';
          },
        },
      });

      var control = $funSelect[0].selectize;
      selectizeObjects[elem.attr('data-element')] = control;

      //report options tally
      var message = options.length + ' option in list';
      if (options.length != 1) {
        message = options.length + ' options in list';
      }

      report_call_feedback(elem, message, 'success');

      //set value
      var elemValue = elem.attr('data-currentValue');
      if (elemValue) {
        for (var p in control.options) {
          if (elemValue == p) {
            control.setValue(p);
          }
        }
      }
    }
  });
}

/**
 * Updates the feedback indicator nodes inside a `.ontology-parent` element
 * with a status message.  Sets a colour class (blue for success, red for error)
 * on the icon node.
 *
 * @param {jQuery} elem     - Any element within the `.ontology-parent` container.
 * @param {string} feedback - The message text to display.
 * @param {string} status   - Status string: `"success"` or `"error"`.
 */
function report_call_feedback(elem, feedback, status) {
  var iconNode = elem.closest('.ontology-parent').find('.copo-tag');
  var contentNode = elem.closest('.ontology-parent').find('.copo-tag-2');
  iconNode.html('Feedback...');

  contentNode.html(feedback);

  iconNode.removeClass('green red blue');

  if (status == 'error') {
    iconNode.addClass('red');
    iconNode.html('Error');
  } else if (status == 'success') {
    iconNode.addClass('blue');
  }
}

/**
 * Populates the detail panel (`.copo-tag-content`) inside an ontology parent
 * with a structured list of schema fields from the selected value object.
 * Only fields marked `show_in_table` are rendered.
 *
 * @param {jQuery} elem        - Any element within the `.ontology-parent` container.
 * @param {Object} onto_value  - The selected value object; pass an empty object to clear the display.
 */
function set_general_ontology_detail(elem, onto_value) {
  var contentNode = elem.closest('.ontology-parent').find('.copo-tag-content');
  contentNode.hide();

  var iconNode = elem.closest('.ontology-parent').find('.copo-tag');
  iconNode.removeClass('green red blue');

  if ($.isEmptyObject(onto_value)) {
    contentNode.html('No option selected');
    return false;
  }

  var schema = JSON.parse(
    elem.closest('.ontology-parent').find('.elem-fields').val()
  );
  var result = $('<div/>', {
    class: 'limit-text',
  });

  contentNode.html(result);

  let message = $(
    '<div class="webpop-content-div" style="padding: 5px;"></div>'
  );
  var codeList = $('<div class="ui relaxed divided list"></div>');
  message.append(codeList);

  for (var i = 0; i < schema.length; ++i) {
    var schemaNode = schema[i];
    if (
      schemaNode.hasOwnProperty('show_in_table') &&
      schemaNode.show_in_table.toString().toLowerCase() == 'true'
    ) {
      var itemNode = $('<div class="item"></div>');
      codeList.append(itemNode);
      itemNode.append(
        '<div class="content"><div class="header">' +
          schemaNode.label +
          '</div><div class="description webpop-content-div">' +
          onto_value[schemaNode.id] +
          '</div></div>'
      );
    }
  }

  result.append(message);
  iconNode.html('Click for info...');
  iconNode.addClass('green');
  // contentNode.slideToggle("slow");
}

/**
 * Updates the ontology icon within an `.onto-label` element to reflect whether
 * the current value is a properly resolved ontology term or a free-text entry.
 * Shows/hides `.free-text` and `.non-free-text` icons accordingly and, for
 * resolved terms, appends the ontology source and an accession hyperlink.
 *
 * @param {jQuery} elem        - Any element within the `.ontology-parent` container.
 * @param {string} onto_object - JSON string of the current ontology value object,
 *                               expected to contain `termAccession` and `termSource`.
 */
function set_ontology_icon(elem, onto_object) {
  var freeText = 'Value not set or free-text value not resolved to an ontology';

  try {
    onto_object = JSON.parse(onto_object);

    if (typeof onto_object === 'object' && onto_object.termAccession != '') {
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.free-text')
        .hide();
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.non-free-text')
        .show();
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .prop('title', 'Ontology field - click for info');
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.onto-label-more')
        .html('')
        .append('<div></div>');

      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.onto-label-more')
        .append(
          '<div style="margin-top: 5px;">Ontology source: ' +
            onto_object.termSource +
            '</div> '
        );
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.onto-label-more')
        .append(
          '<a href="' +
            onto_object.termAccession +
            '" target="_blank">' +
            onto_object.termAccession +
            '</a> '
        );
    } else {
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.free-text')
        .show();
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .find('.non-free-text')
        .hide();
      elem
        .closest('.ontology-parent')
        .find('.onto-label')
        .prop('title', freeText);
    }
  } catch (e) {
    elem
      .closest('.ontology-parent')
      .find('.onto-label')
      .find('.free-text')
      .show();
    elem
      .closest('.ontology-parent')
      .find('.onto-label')
      .find('.non-free-text')
      .hide();
    elem
      .closest('.ontology-parent')
      .find('.onto-label')
      .prop('title', freeText);
  }
}

/**
 * Initialises Selectize on all `.copo-multi-search` elements that have not
 * already been instantiated.  Renders component-linked records from a
 * pre-loaded options list and syncs the selection to a hidden `.copo-multi-values`
 * element.
 */
function refresh_multisearch() {
  $('.copo-multi-search').each(function () {
    var elem = $(this);

    if (!/selectize/i.test(elem.attr('class'))) {
      // if not already instantiated
      var valueElem = elem
        .closest('.copo-form-group')
        .find('.copo-multi-values');
      var elemSpecs = JSON.parse(
        elem.closest('.copo-form-group').find('.elem-json').val()
      );
      var maxTems = 'null'; //maximum selectable items
      var component = elem.attr('data-component');

      if (valueElem.is('[data-maxItems]')) {
        maxTems = valueElem.attr('data-maxItems');
      }

      var $funSelect = elem.selectize({
        onChange: function (value) {
          if (value) {
            valueElem.val(value).trigger('change');
          } else {
            valueElem.val('');
          }

          $('.selectize-control.copo-multi-search')
            .closest('.copo-form-group')
            .webuiPopover('destroy');
        },
        onBlur: function () {
          $('.selectize-control.copo-multi-search')
            .closest('.copo-form-group')
            .webuiPopover('destroy');
        },
        // dropdownParent: 'body',
        maxItems: maxTems,
        persist: true,
        create: false,
        plugins: ['remove_button'],
        valueField: elemSpecs.value_field,
        labelField: elemSpecs.label_field,
        searchField: elemSpecs.search_field,
        options: elemSpecs.options,
        render: {
          item: function (item, escape) {
            return (
              '<div>' +
              (item[elemSpecs.label_field]
                ? '<span>' + escape(item[elemSpecs.label_field]) + '</span>'
                : '') +
              '</div>'
            );
          },
          option: function (item, escape) {
            var label = ''; // item[elemSpecs.label_field];
            var caption = '<div>';
            for (var i = 0; i < elemSpecs.secondary_label_field.length; ++i) {
              caption +=
                '<div>' + item[elemSpecs.secondary_label_field[i]] + '</div>';
            }
            caption += '</div>';

            return (
              '<div>' +
              '<span class="caption caption-component" data-component="' +
              component +
              '">' +
              escape(label) +
              '</span>' +
              (caption ? '<div class="caption">' + caption + '</div>' : '') +
              '</div>'
            );
          },
        },
      });

      var control = $funSelect[0].selectize;
      control.setValue(valueElem.val().split(',')); //set default value

      //retain reference to control for any future reference
      selectizeObjects[valueElem.attr('id')] = control;
    }
  });
}

// ═══ AUTOCOMPLETE ═════════════════════════════════════════════════════════════

/**
 * Bootstraps the OLS (Ontology Lookup Service) autocomplete widget on all
 * `.ontology-field` inputs.  Removes any existing autocomplete dropdowns first
 * to prevent duplicates on re-initialisation.
 *
 * Inner callbacks:
 * - `do_pre`      — Shows a loading spinner before the request is sent.
 * - `do_select`   — Handles term selection, writing values to the appropriate
 *                   fields depending on annotator type (`txt`, `ss`, or default).
 * - `do_position` — No-op positional callback required by the AutoComplete API.
 * - `do_post`     — Parses the OLS JSON response and builds the suggestion list.
 */
var auto_complete = function () {
  // remove all previous autocomplete divs
  $('.autocomplete').remove();
  AutoComplete(
    {
      EmptyMessage: 'No Annotations Found',
      Url: $('#elastic_search_ajax').val(),
      _Select: do_select,
      _Render: do_post,
      _Position: do_position,
      _Pre: do_pre,
    },
    '.ontology-field'
  );

  function do_pre() {
    // make loading spinner visible before request to OLS
    $(this.Input).siblings('.input-group-addon').css('visibility', 'visible');
    // we can also make changes to the value sent OLS here if needs be
    return this.Input.value;
  }

  function do_select(item) {
    if ($(document).data('annotator_type') == 'txt') {
      $('#annotator-field-0').val(
        $(item).data('annotation_value') +
          ' :-: ' +
          $(item).data('term_accession')
      );
    } else if ($(document).data('annotator_type') == 'ss') {
      // this function defined in copo_annotations.js
      append_to_annotation_list(item);
    } else {
      $(this.Input).val($(item).data('annotation_value'));
      $(this.Input)
        .closest('.ontology-parent')
        .find("[id*='termSource']")
        .val($(item).data('term_source'));
      $(this.Input)
        .closest('.ontology-parent')
        .find("[id*='termAccession']")
        .val($(item).data('term_accession'));
    }
  }

  function do_position(a, b, c) {}

  function do_post(response) {
    response = JSON.parse(response);

    var properties = Object.getOwnPropertyNames(response);

    var empty,
      length = response.length,
      li = document.createElement('li'),
      ul = document.createElement('ul');

    for (var item in response.response.docs) {
      doc = response.response.docs[item];

      try {
        var s;
        s = response.highlighting[doc.id].label_autosuggest[0];
        if (s == undefined) {
          s = response.highlighting[doc.id].synonym;
        }
        var short_form;
        var desc = doc.description;
        if (desc == undefined) {
          desc = 'Description Not Available';
        }
        if (doc.ontology_prefix == undefined) {
          short_form = 'Origin Unknown';
        } else {
          short_form = doc.ontology_prefix;
        }
        li.innerHTML =
          '<span title="' +
          doc.iri +
          ' - ' +
          desc +
          '" class="ontology-label label label-info"><span class="ontology-label-text"><img src="/static/assets/img/ontology.png"/>' +
          doc.ontology_prefix +
          ' : ' +
          s +
          ' ' +
          '</span>' +
          ' - ' +
          '<span class="ontology-description">' +
          desc +
          '</span></span>';

        $(li).attr('data-id', doc.id);
        var styles = {
          margin: '2px',
          marginTop: '4px',
          fontSize: 'large',
        };
        $(li).css(styles);
        $(li).attr('data-term_accession', doc.iri);

        $(li).attr('data-annotation_value', doc.label);

        $(li).attr('data-term_source', short_form);

        ul.appendChild(li);
        li = document.createElement('li');
      } catch (err) {
        console.log(err);
        li = document.createElement('li');
      }
    }
    $(this.DOMResults).empty();
    this.DOMResults.append(ul);
    $(this.Input).siblings('.input-group-addon').css('visibility', 'hidden');
  }
}; //end of function

/**
 * Checks whether a value is present in an array.
 *
 * @param {*}     value - The value to search for.
 * @param {Array} array - The array to search within.
 * @returns {boolean} `true` if the value exists in the array, otherwise `false`.
 */
function isInArray(value, array) {
  return array.indexOf(value) > -1;
}

// ═══ DATA DISPLAY PANEL BUILDERS ═════════════════════════════════════════════

/**
 * Builds an HTML string representing a collapsible list panel for use inside
 * a DataTable cell.  Handles arrays of strings, arrays of arrays, arrays of
 * objects, and plain objects.
 *
 * @param {Array|Object} itemData - The data to render; returns an empty string if falsy.
 * @param {string}       link     - A unique slug used to generate element IDs.
 * @returns {string} An HTML string containing the rendered panel.
 */
function get_data_list_panel(itemData, link) {
  if (!itemData) {
    return '';
  }

  var containerFuild = $('<div/>', {
    class: 'container-fluid',
  });

  var containerRow = $('<div/>', {
    class: 'row',
    style: 'padding:1px;',
  });

  var containerColumn = $('<div/>', {
    class: 'col-sm-12 col-md-12 col-lg-12',
    style: 'padding:1px;',
  });

  var mainMenuDiv = $('<div/>', {
    id: 'mainMenu_' + link,
  });

  var listGroupPanel = $('<div/>', {
    class: 'list-group panel',
    style: 'margin-bottom: 0px; border: 0 solid transparent;',
  });

  var topLevelLink = $('<a></a>').attr({
    href: '#demo3',
    class: 'list-group-item',
    'data-toggle': 'collapse',
    'data-parent': '#MainMenu',
    style: 'background: #ebf0fa;',
  });

  var topLevelDiv = $('<div/>', {
    class: 'collapse',
  });

  var subMenuLink = $('<a/>', {
    href: '#',
    class: 'list-group-item',
    click: function (event) {
      event.preventDefault();
      return false;
    },
  });

  if (Object.prototype.toString.call(itemData) === '[object Array]') {
    $.each(itemData, function (key, val) {
      if (Object.prototype.toString.call(val) === '[object String]') {
        var ctrlElem = subMenuLink.clone();
        ctrlElem.html(val);
        listGroupPanel.append(ctrlElem);
      } else if (Object.prototype.toString.call(val) === '[object Array]') {
        var ctrlElemLink = topLevelLink.clone();
        ctrlElemLink.attr('href', '#rec_' + link + '_' + key);
        ctrlElemLink.html(
          '<span>Item ' +
            (key + 1) +
            '</span><i class="fa fa-caret-down pull-right"></i>'
        );

        listGroupPanel.append(ctrlElemLink);

        var ctrlElemDiv = topLevelDiv.clone();
        ctrlElemDiv.attr('id', 'rec_' + link + '_' + key);
        ctrlElemDiv.attr('class', 'collapse');

        $.each(val, function (key2, val2) {
          var spl = [];
          var displayedValue = '';
          var ctrlElemSubLink = subMenuLink.clone();
          if (Object.prototype.toString.call(val2) === '[object String]') {
            displayedValue = val2;
          } else if (
            Object.prototype.toString.call(val2) === '[object Array]'
          ) {
            $.each(val2, function (key22, val22) {
              spl.push(val22);
            });
            displayedValue = spl.join('<br/>');

            if (key2 == 0) {
              ctrlElemLink.find('span').html(displayedValue);
              displayedValue = '';
            }
          } else if (
            Object.prototype.toString.call(val2) === '[object Object]'
          ) {
            Object.keys(val2).forEach(function (k2) {
              spl.push(k2 + ': ' + val2[k2]);
            });

            displayedValue = spl.join('<br/>');

            if (key2 == 0) {
              ctrlElemLink.find('span').html(displayedValue);
              displayedValue = '';
            }
          }

          if (displayedValue) {
            ctrlElemSubLink.html(displayedValue);
            ctrlElemDiv.append(ctrlElemSubLink);
          }
        });

        listGroupPanel.append(ctrlElemDiv);
      } else if (Object.prototype.toString.call(val) === '[object Object]') {
      }
    });
  } else if (Object.prototype.toString.call(itemData) === '[object Object]') {
    //not an array-type object
    $.each(itemData, function (key, val) {
      var ctrlElem = subMenuLink.clone();
      ctrlElem.html(key + ': ' + val);

      listGroupPanel.append(ctrlElem);
    });
  }

  containerColumn.append(mainMenuDiv.append(listGroupPanel));
  containerRow.append(containerColumn);
  containerFuild.append(containerRow);

  return $('<div/>').append(containerFuild).html();
}

/**
 * Builds an HTML string containing a collapsible button/badge trigger and its
 * associated hidden content panel, used in DataTable cells that hold array data.
 *
 * @param {string} link      - Unique ID used for the collapse target element.
 * @param {string} itemData  - Inner HTML of the collapse body (typically from `get_data_list_panel`).
 * @param {number} itemCount - Number of items; drives the badge text and singular/plural label.
 * @returns {string} An HTML string for the collapse button and panel.
 */
function get_data_item_collapse(link, itemData, itemCount) {
  // create badge
  var badgeSpan = $('<span/>', {
    class: 'badge',
    style:
      'background: #fff;  border-radius: 5px; margin-left: 2px; margin-bottom: 1px;',
    html: itemCount,
  });

  //create button
  var itemsLanguage = 'items';
  if (itemCount == 1) {
    itemsLanguage = 'item';
  }
  var collapseBtn = $('<a></a>')
    .attr({
      class: 'btn btn-xs btn-info',
      'data-toggle': 'collapse',
      'data-target': '#' + link,
      style:
        'margin-left:1px; margin-bottom:0px; border-radius:0; background-image:none; border-color:transparent; ',
      type: 'button',
    })
    .html(
      $('<div/>').append(badgeSpan).html() +
        '<span style="margin-left: 2px; margin-bottom:0px;"> ' +
        itemsLanguage +
        '</span>'
    );

  var collapseDiv = $('<div>', {
    id: link,
    class: 'collapse',
    html: itemData,
  });

  var ctrlDiv = $('<div/>').append(collapseBtn).append(collapseDiv);

  return ctrlDiv.html();
}

/**
 * Converts a camelCase string to a human-readable title-case sentence.
 * The first word is capitalised; subsequent words are lower-cased.
 *
 * @param {string} xter - The camelCase string to format.
 * @returns {string} A space-separated, title-case string.
 */
function format_camel_case(xter) {
  var a = xter.replace(/([A-Z])/g, ' $1').replace(/^./, function (str) {
    return str.toUpperCase();
  });

  var refinedXter = a.trim().split(/\s+/g);

  for (var i = 1; i < refinedXter.length; ++i) {
    var str = refinedXter[i];
    str = str.toLowerCase().replace(/\b[a-z]/g, function (letter) {
      return letter.toLowerCase();
    });

    refinedXter[i] = str;
  }

  return refinedXter.join(' ');
}

/**
 * Builds a jQuery table element representing datafile description metadata.
 * This is a specialised counterpart to `build_attributes_display()` that
 * operates on the `data.description` structure rather than `data.component_attributes`.
 *
 * @param {Object} data                         - Server response from the visualise endpoint.
 * @param {Object} data.description             - Description payload.
 * @param {Array}  data.description.columns     - Column definitions with `title` and `data` keys.
 * @param {Object} data.description.data_set    - Key/value map of field names to display values.
 * @returns {jQuery} A jQuery-wrapped `<table>` element.
 */
function build_description_display(data) {

  var resolvedTable = $('<table/>');

  for (var j = 0; j < data.description.columns.length; ++j) {
    var Ddata = data.description.columns[j];

    var iRow = $('<tr/>', {
      class: 'copo-webui-tabular',
    });

    var labelCol = $('<td/>').attr('colspan', 2).append(Ddata.title);
    iRow.append(labelCol);

    var dataCol = $('<td/>')
      .attr('colspan', 2)
      .append(data.description.data_set[Ddata.data]);
    iRow.append(dataCol);

    resolvedTable.append(iRow);
  }

  return resolvedTable;
}

/**
 * Builds a jQuery summary table from a component's attribute data, typically
 * rendered inside a webuiPopover when viewing record details.
 *
 * @param {Object} data                                    - Server response from the visualise endpoint.
 * @param {Object} data.component_attributes               - Attribute payload.
 * @param {Array}  data.component_attributes.columns       - Column definitions with `title` and `data` keys.
 * @param {Object} data.component_attributes.data_set      - Key/value map of field names to display values.
 * @returns {jQuery} A jQuery-wrapped `<table>` element styled with `.summary-details-table`.
 */
function build_attributes_display(data) {
  var contentHtml = $('<table/>', {
    // cellpadding: "5",
    cellspacing: '0',
    border: '0',
    class: 'summary-details-table',
    // style: "padding-left:50px;"
  });

  if (data.component_attributes.columns) {
    // expand row

    for (var i = 0; i < data.component_attributes.columns.length; ++i) {
      var colVal = data.component_attributes.columns[i];

      var colTR = $('<tr/>');
      contentHtml.append(colTR);

      colTR
        .append($('<td/>').append(colVal.title))
        .append(
          $('<td/>').append(data.component_attributes.data_set[colVal.data])
        );
    }
  }

  return contentHtml;
}

/**
 * Builds and returns a cloned Bootstrap collapsible panel structure
 * (panel-group > panel > panel-heading > panel-body).
 *
 * @param {string} [panelType='default'] - Bootstrap panel context class suffix
 *   (e.g. `'default'`, `'primary'`, `'info'`).
 * @returns {jQuery} A cloned jQuery element containing the full panel structure.
 */
function get_collapsible_panel(panelType) {
  if (!panelType) {
    panelType = 'default';
  }

  var panelGroup = $('<div/>', {
    class: 'panel-group',
  });

  var panelClass = 'panel panel-' + panelType;
  var panel = $('<div/>', {
    class: panelClass,
  });

  var panelHeading = $('<div/>', {
    class: 'panel-heading',
  });

  var panelTitle = $('<div/>', {
    class: 'panel-title',
  });

  var panelTitleAnchor = $('<a/>', {
    'data-toggle': 'collapse',
  });

  panelTitle.append(panelTitleAnchor);
  panelHeading.append(panelTitle);

  panel.append(panelHeading);

  var panelCollapse = $('<div/>', {
    class: 'panel-collapse collapse',
  });

  var panelBody = $('<div/>', {
    class: 'panel-body',
  });

  panelCollapse.append(panelBody);

  panel.append(panelCollapse);

  panelGroup.append(panel);

  return $('<div/>').append(panelGroup).clone();
}

/**
 * Builds and returns a cloned Bootstrap panel structure with heading, body,
 * and footer sections.
 *
 * @param {string} [panelType='default'] - Bootstrap panel context class suffix.
 * @returns {jQuery} A cloned jQuery element containing the panel.
 */
function get_panel(panelType) {
  if (!panelType) {
    panelType = 'default';
  }

  var panelClass = 'panel panel-' + panelType;
  var panel = $('<div/>', {
    class: panelClass,
  });

  var panelHeading = $('<div/>', {
    class: 'panel-heading',
    style: 'background-image: none;',
  });

  panel.append(panelHeading);

  var panelBody = $('<div/>', {
    class: 'panel-body',
  });

  panel.append(panelBody);

  var panelFooter = $('<div/>', {
    class: 'panel-footer',
    style: 'background-color: #fff;',
  });

  panel.append(panelFooter);

  return $('<div/>').append(panel).clone();
}

// ═══ COMPONENT NAVIGATION AND ICON MANAGEMENT ════════════════════════════════

/**
 * Retrieves the metadata definition object for a named component from the
 * global `component_def` dictionary.
 *
 * @param {string} componentName - The component identifier (case-insensitive).
 * @returns {Object|undefined} The component definition object, or `undefined`
 *   if the component is not registered.
 */
function get_component_meta(componentName) {
  return component_def[componentName.toLowerCase()];
}

/**
 * Entry point for building the component-page navbar.
 * Generates navigation controls, initialises the component dropdown menu,
 * and sets the component icon in the page header.
 *
 * @param {string} componentName - The active component identifier.
 */
function do_page_controls(componentName) {
  const profile_type = $('#profile_type').val();
  generate_component_control(componentName, profile_type);
  initialiseComponentDropdownMenu();
  setComponentIcon(componentName);
}

/**
 * Sets the component icon in the page header based on the component's
 * `materialIcon` or `semanticIcon` definition.  For profile pages the icon
 * is skipped.  Also triggers the component welcome message if a template exists.
 *
 * @param {string} componentName - The active component identifier.
 */
function setComponentIcon(componentName) {
  let $componentIcon = $('#componentIcon');
  let component = get_component_meta(componentName);

  if (componentName === 'profile') return;

  if (component && $componentIcon.length) {
    if (component.materialIcon) {
      $componentIcon.removeClass('ui icon').addClass(`material-symbols-outlined ${component.color}`).text(component.materialIcon);
    } else {
      $componentIcon.addClass(`${component.semanticIcon} ${component.color}`);
    }

    // Update page welcome message if a template message exists
    addComponentMessage(componentName);
  } else {
    if (!$componentIcon.length) {
      // console.warn(`Component icon not found for ${componentName}`);
      return;
    }

    $componentIcon.hide();
    // Remove the brackets surrounding the icon
    $componentIcon
      .contents()
      .filter(function () {
        return this.nodeType === 3;
      })
      .remove();
  }
}

/**
 * Returns the normalised group name for a component, or an empty string if the
 * component has no group or its group is the sentinel value `"None"`.
 *
 * @param {string} componentName - The component identifier.
 * @returns {string} The lower-cased, trimmed group name, or `''`.
 */
function getComponentGroupName(componentName) {
  let component = get_component_meta(componentName);
  if (!component) return '';

  // Treat null, undefined, empty string or string "None" as empty.
  // Parent components would typically have a "None" group name.
  return !component.groupName || component.groupName === 'None'
    ? ''
    : component.groupName.trim().toLowerCase();
}

/**
 * Groups an array of component definition objects by their `groupName` field.
 * Components without a group (or with `groupName === "None"`) are collected
 * under the empty-string key `''`.
 *
 * @param {Object[]} components - Array of component definition objects.
 * @returns {Object} A plain object mapping group name strings to arrays of components.
 */
function groupComponentsByGroupName(components) {
  const grouped = {};

  components.forEach((component) => {
    // Treat null, undefined, empty string or string "None" as empty.
    // Parent components would typically have a "None" group name.
    const group =
      !component.groupName || component.groupName === 'None'
        ? ''
        : component.groupName.trim().toLowerCase();
    if (!grouped[group]) {
      grouped[group] = [];
    }

    grouped[group].push(component);
  });

  return grouped;
}

/**
 * Creates a cloned anchor element from the appropriate page template
 * (icon-only or button) and populates it with the component's title, href,
 * icon class / material icon text, and button label.
 *
 * @param {Object}  item           - Component definition object.
 * @param {string}  profileId      - The active profile's MongoDB ObjectId, used to resolve `item.url`.
 * @param {boolean} [isIconOnly=false] - When `true`, clones the icon-only template; otherwise the button template.
 * @returns {jQuery} The populated anchor element.
 */
function createComponentAnchor(item, profileId, isIconOnly = false) {
  const componentGroupName = item.groupName ? ` ${item.groupName}` : '';
  const $templateAnchor = isIconOnly
    ? $('a.pcomponent-icon-template').clone()
    : $('a.pcomponent-button-template').clone();

  $templateAnchor.attr('title', function (_, oldTitle) {
    return `${oldTitle || ''} ${item.title}${componentGroupName}`.trim();
  });
  $templateAnchor.attr(
    'href',
    item.url ? item.url.replace('999', profileId) : '#'
  );

  const $icon = $templateAnchor.find('i');

  if (isIconOnly) {
    if (item.materialIcon) {
      $icon.removeClass('ui icon').addClass(`material-symbols-outlined pcomponent-material-icon ${item.color}`).text(item.materialIcon);
    } else {
      $icon.addClass(`${item.color} ${item.semanticIcon}`);
    }
    $templateAnchor.removeClass('pcomponent-icon-template');
  } else {
    const $button = $templateAnchor.find('.pcomponent-button');
    $button.addClass(item.color);
    if (item.materialIcon) {
      $button.find('.pcomponent-icon').removeClass('pcomponent-icon').addClass('material-symbols-outlined pcomponent-material-icon pcomponent-icon').text(item.materialIcon);
      $icon.removeClass('pcomponent-icon').addClass('material-symbols-outlined pcomponent-material-icon').text(item.materialIcon);
    } else {
      $button.find('.pcomponent-icon').addClass(item.iconClass);
      $icon.addClass(item.iconClass);
    }
    $button
      .find('.pcomponent-name')
      .text(`${item.buttonLabel}${componentGroupName}`);
    $templateAnchor.removeClass('pcomponent-button-template');
  }

  return $templateAnchor;
}

/**
 * Returns `true` if `item` matches the currently active component, so it can
 * be omitted from the navigation icon/button list.
 *
 * @param {Object} item - Component definition object to test.
 * @returns {boolean} Whether the item is the current page's component.
 */
function skipCurrentComponent(item) {
  const componentName = $('#nav_component_name').val();
  const component = get_component_meta(componentName);
  // Skip if it's the current component
  return (
    item.component === component.component && item.title === component.title
  );
}

/**
 * Clones the appropriate dropdown wrapper template for a component group and
 * populates it with child component anchors.
 *
 * @param {string}   groupName      - The normalised group name; used to locate the template class.
 * @param {Object[]} childrenItems  - Array of component definition objects for the group's children.
 * @param {string}   profileId      - The active profile's MongoDB ObjectId.
 * @param {boolean}  isIconOnly     - Whether to use icon-only templates instead of button templates.
 * @returns {jQuery|undefined} The populated wrapper element, or `undefined` if no template was found.
 */
function createDropdownWrapper(
  groupName,
  childrenItems,
  profileId,
  isIconOnly
) {
  // Normalise group name for comparison
  const targetParentComponentName = String(groupName).trim().toLowerCase();

  // Find dropdown wrapper based on template type
  const templateDiv = isIconOnly
    ? `.profile-dropdown-wrapper.${targetParentComponentName}-pcomponent-dropdown-icon-template`
    : `.profile-dropdown-wrapper.${targetParentComponentName}-pcomponent-dropdown-button-template`;

  const $wrapper = $(templateDiv).clone();

  if (!$wrapper.length) {
    console.error(`Component dropdown wrapper not found for ${groupName}`);
    return;
  }

  const $menu = $wrapper.find('.profile-dropdown-menu');
  let $container = null;

  // Remove template-specific classes
  if (isIconOnly) {
    $wrapper.removeClass(
      `${targetParentComponentName}-pcomponent-dropdown-icon-template`
    );
  } else {
    // Inner container is within component button only
    $container = $menu.find('.item');

    $wrapper.removeClass(
      `${targetParentComponentName}-pcomponent-dropdown-button-template`
    );
  }

  // Build child components/subcomponents
  childrenItems.forEach((item) => {
    const childAnchor = createComponentAnchor(item, profileId, isIconOnly);
    ($container || $menu).append(childAnchor);
  });

  if ($container) $menu.append($container);

  return $wrapper;
}

/**
 * Generates and injects the full suite of page header controls for a component:
 * profile title, page title, optional sidebar panels, page-level action buttons,
 * and the profile component icon/button row.  Components are grouped and rendered
 * as dropdowns where applicable.
 *
 * @param {string} componentName - The active component identifier.
 * @param {string} profile_type  - The active profile type string (e.g. `"dtol"`).
 */
function generate_component_control(componentName, profile_type) {
  var component = get_component_meta(componentName);
  var pageHeaders = $('.copo-page-headers'); //page header/icons
  var pageIcons = $('.copo-page-icons'); //profile component icons
  var sideBar = $('.copo-sidebar'); //sidebar panels
  const invalidValues = ['none', 'null', 'undefined', ''];

  var profile_id = '';
  if ($('#profile_id').length) {
    profile_id = $('#profile_id').val();
  }

  //add profile title
  const $profileTitleElement = $('#profile_title');
  if ($profileTitleElement.length && !invalidValues.includes($profileTitleElement.val().toLowerCase())) {
    const tourId =
      profile_type ?
        profile_type_def[profile_type.toLowerCase()]?.tourId || ''
      : '';
    let profileTitle = $profileTitleElement.val();
    let $profileTitleDiv = $('<div/>', {
      class: 'page-title-custom',
      html: `<span class='profile-title' title='${profileTitle}' data-tour-id='${tourId}'>Profile: ${profileTitle}</span>`,
    });

    pageHeaders.append($profileTitleDiv);
  }

  //add page title
  let componentGroupName = getComponentGroupName(componentName);
  var pageTitle = $('<span/>', {
    class: 'page-title-custom',
    html:
      `<span class='page-title-text'>${component.title}&nbsp;${componentGroupName}</span>` +
      (component.subtitle
        ? "<span class='page-subtitle-text' data-tour-id='component_options component_options_with_data_uploaded'>" +
          $(component.subtitle).val() +
          '</span>'
        : ''),
  });

  pageHeaders.append(pageTitle);

  //create panels
  if (component.sidebarPanels) {
    var sidebarPanels = $('.copo-sidebar-templates').clone();
    var sidebarPanels2 = sidebarPanels.clone();
    sidebarPanels.find('.nav-tabs').html('');
    sidebarPanels.find('.tab-content').html('');
    $('.copo-sidebar-templates').remove();

    component.sidebarPanels.forEach(function (item) {
      sidebarPanels
        .find('.nav-tabs')
        .append(sidebarPanels2.find('.nav-tabs').find('.' + item));
      sidebarPanels
        .find('.tab-content')
        .append(sidebarPanels2.find('.tab-content').find('.' + item));
    });

    sideBar
      .prepend(sidebarPanels.find('.tab-content'))
      .prepend(sidebarPanels.find('.nav-tabs'));
  }

  //create buttons
  var buttonsSpan = $('<span/>', { style: 'white-space:nowrap;' });
  pageHeaders.append(buttonsSpan);
  if (component.buttons) {
    component.buttons.forEach(function (item) {
      button_str = title_button_def[item.split('|')[0]].template;
      additional_attr = title_button_def[item.split('|')[0]].additional_attr;
      button = $(button_str);

      if (additional_attr != undefined) {
        attrs = additional_attr.split(',');
        for (var i = 0; i < attrs.length; ++i) {
          if (attrs[i].indexOf(':') > -1) {
            key = attrs[i].split(':')[0];
            value = attrs[i].split(':')[1];
            if (value.indexOf('#') > -1 || value.indexOf('.') > -1) {
              button.attr(key, $(value).val());
            } else {
              button.attr(key, value);
            }
          }
        }
      }
      buttonsSpan
        .append(button)
        .append("<span style='display: inline;'>&nbsp;</span>");
    });
  }

  //...and profile component buttons
  if (profile_type != undefined) {
    var pcomponentHTML = $('.pcomponents-icons-templates')
      .clone()
      .removeClass('pcomponents-icons-templates');
    pcomponentHTML.find('.pcomponents-anchor').remove();
    pageIcons.append(pcomponentHTML);

    var components = get_profile_components(profile_type);
    if (components.length === 0) return; // No components so skip

    // Group components by 'group' field value
    const grouped = groupComponentsByGroupName(components);

    Object.entries(grouped).forEach(([groupName, groupItems]) => {
      // Skip current component before deciding dropdown logic
      const filteredItems = groupItems.filter(
        (item) => !skipCurrentComponent(item)
      );

      // Render as dropdown if group is non-empty and has more
      // than one subcomponent
      const isDropdownMenu = groupName && filteredItems.length > 1;

      if (isDropdownMenu) {
        // Components with subcomponents i.e. dropdown menus
        const dropdown = createDropdownWrapper(
          groupName,
          filteredItems,
          profile_id,
          true
        );
        pcomponentHTML.append(dropdown);
      } else {
        // Single/Standalone components
        filteredItems.forEach((item) => {
          const anchor = createComponentAnchor(item, profile_id, true);
          pcomponentHTML.append(anchor);
        });
      }
    });
  } else {
    // Do not display the profile component icons container
    // if profile type is not defined
    pageIcons.hide();
  }

  //refresh components...
  refresh_tool_tips();
}

// ═══ WEBUI POPOVER AND HELP SYSTEM ════════════════════════════════════════════

/**
 * Initialises or refreshes a webuiPopover on an element.  Extra configuration
 * keys supplied via `exrta_meta` are merged into the base config.
 *
 * @param {jQuery} elem       - The element to attach the popover to.
 * @param {string} title      - The popover title.
 * @param {string} content    - The popover body HTML (wrapped in `.webpop-content-div`).
 * @param {Object} exrta_meta - Additional webuiPopover config options to merge.
 */
function refresh_webpop(elem, title, content, exrta_meta) {
  var config = {
    title: title,
    content: '<div class="webpop-content-div">' + content + '</div>',
    closeable: true,
    cache: false,
    width: 300,
    trigger: 'hover',
    arrow: false,
    animation: 'fade',
    placement: 'right',
    dismissible: false,
    onHide: function ($element) {
      WebuiPopovers.updateContent(
        elem,
        '<div class="webpop-content-div">' + content + '</div>'
      );
      elem.removeClass('copo-form-control-focus');
    },
    onShow: function ($element) {
      elem.addClass('copo-form-control-focus');
    },
  };

  //refresh config with extra configurations
  $.each(exrta_meta, function (key, val) {
    config[key] = val;
  });

  elem.webuiPopover(config);
}

/**
 * Globally enables or disables the per-field help tip popovers within a section.
 * When disabled, existing popovers are destroyed and a `data-helptip="no"` flag
 * is set to prevent new ones from opening.
 *
 * @param {boolean} state         - `true` to enable help tips; `false` to disable.
 * @param {jQuery}  parentElement - The container element to scope the toggle to.
 */
function toggle_display_help_tips(state, parentElement) {
  if (!state) {
    parentElement.find('.copo-form-group').webuiPopover('destroy');
    parentElement.find('.copo-form-group').attr('data-helptip', 'no');
  } else {
    parentElement.find('.copo-form-group').attr('data-helptip', 'yes');
  }
}

/**
 * Returns a centred Font Awesome spinner element for use as a loading indicator.
 *
 * @returns {jQuery} A cloned jQuery `<div>` containing the spinner.
 */
function get_spinner_image() {
  var loaderObject = $('<div>', {
    style: 'text-align: center',
    html: "<span class='fa fa-spinner fa-pulse fa-3x'></span>",
  });

  return loaderObject.clone();
}

/**
 * Normalises a raw context-help payload into a flat dataset array suitable
 * for rendering in a DataTable.  Assigns sequential IDs and generates unique
 * DOM-safe identifiers for each help entry.
 *
 * @param {Object}   contextHelpList            - Raw help payload from the server.
 * @param {Object[]} contextHelpList.properties - Array of `{title, content, context}` objects.
 * @returns {Object[]} Normalised array of help items with `id`, `title`, `content`, `context`, and `help_id`.
 */
function sanitise_help_list(contextHelpList) {
  var dataSet = [];

  if (contextHelpList.properties) {
    var dtd = contextHelpList.properties;

    for (var i = 0; i < dtd.length; ++i) {
      var option = {};
      option['id'] = i + 1;
      option['title'] = dtd[i].title;
      option['content'] = dtd[i].content;
      option['context'] = dtd[i].context;
      var helpID = Math.random() + Math.random() + Math.random();
      helpID = helpID.toString();
      option['help_id'] = 'context_help_' + i + '_' + helpID.replace('.', '_');
      dataSet.push(option);
    }
  }

  return dataSet;
}

/**
 * Renders or refreshes the context-help DataTable (`#page-context-help`).
 * If the table does not exist in the DOM this function is a no-op.
 *
 * @param {Object} data - Raw context-help payload (passed to `sanitise_help_list`).
 */
function do_context_help(data) {
  var tableID = 'page-context-help';
  var helpComponent = $('#' + tableID);

  //if true then page requests context help control to be added
  if (!helpComponent.length) {
    return false;
  }

  var dtd = sanitise_help_list(data);
  var table = null;

  if ($.fn.dataTable.isDataTable('#' + tableID)) {
    table = $('#' + tableID).DataTable();
  }

  if (table) {
    // Refresh existing table data
    table.clear().draw();
    table.rows.add(dtd);
    table.columns.adjust().draw();
    table.search('').columns().search('').draw();
  } else {
    table = $('#' + tableID).DataTable({
      data: dtd,
      searchHighlight: true,
      lengthChange: false,
      order: [[0, 'asc']],
      pageLength: 5,
      language: {
        info: ' _START_ to _END_ of _TOTAL_ topics',
        lengthMenu: '_MENU_ tips',
        search: ' ',
      },
      columns: [
        {
          data: 'id',
          visible: false,
        },
        {
          orderable: false,
          width: '2%',
          data: null,
          render: function (data, type, row, meta) {
            var iconSpan =
              '<span data-target="' +
              data.help_id +
              '" class="side-help-trigger" aria-hidden="true" title="View help content"></span>';

            var parentDiv = $('<div></div>');
            parentDiv.append(iconSpan);

            return $('<div></div>').append(parentDiv).html();
          },
        },
        {
          data: null,
          title: 'Help Topics',
          render: function (data, type, row, meta) {
            var helpTopicID = data.help_id;

            var helpTitleDiv = $('<div></div>')
              .attr('id', 'title_' + helpTopicID)
              .html('<div>' + data.title + '</div>');

            var helpContentDiv = $('<div></div>')
              .attr('id', helpTopicID)
              .attr('class', 'collapse context-help-collapse')
              .css('margin-top', '10px')
              .html('<div>' + data.content + '</div>');

            return $('<div></div>')
              .append(helpTitleDiv)
              .append(helpContentDiv)
              .html();
          },
        },
        {
          data: 'content',
          visible: false,
        },
      ],
      dom: 'lft<"row">rip',
      columnDefs: [
        {
          orderData: 0,
        },
      ],
    });
  }

  $('#' + tableID + '_wrapper')
    .find('.dataTables_filter')
    .find('input')
    .removeClass('input-sm')
    .attr('placeholder', 'Search Help');
  // .attr("size", 30);
}

/**
 * Fetches global help data for a component from the server and passes the
 * context-help section to `do_context_help` for rendering.
 *
 * @param {string} component - The component identifier to request help for.
 */
function do_global_help(component) {
  $.ajax({
    url: copoVisualsURL,
    type: 'POST',
    headers: {
      'X-CSRFToken': csrftoken,
    },
    data: {
      task: 'help_messages',
      component: component,
    },
    success: function (data) {
      //set quick tour message and trigger display event
      try {
        do_context_help(data.context_help);
      } catch (err) {}
    },
    error: function () {
      alert("Couldn't retrieve page help!");
    },
  });
}

/**
 * Returns a formatted timestamp string suitable for use in filenames or IDs.
 * Format: `D_Mon_YYYY_H_M_S` (e.g. `"27_Mar_2026_14_30_00"`).
 *
 * @returns {string} The current date and time as an underscore-delimited string.
 */
function get_timestamp() {
  var date = new Date();

  var monthNames = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
  ];

  var day = date.getDate();
  var monthIndex = date.getMonth();
  var year = date.getFullYear();
  var hour = date.getHours();
  var minute = date.getMinutes();
  var second = date.getSeconds();

  return (
    day +
    '_' +
    monthNames[monthIndex] +
    '_' +
    year +
    '_' +
    hour +
    '_' +
    minute +
    '_' +
    second
  );
}

/**
 * Binds a delegated click handler to `.side-help-trigger` elements in the
 * context-help table.  Toggles the collapse state of the associated help
 * content block and updates the trigger's `shown` class accordingly.
 */
function do_context_help_event() {
  $(document).on('click', '.side-help-trigger', function (e) {
    var dataTargetID = $(this).attr('data-target');

    if ($(this).parent().hasClass('shown')) {
      $(this).parent().removeClass('shown');
      $('#' + dataTargetID).collapse('hide');
    } else {
      $(this).parent().addClass('shown');
      $('#' + dataTargetID).collapse('show');
    }
  });
}

/**
 * Binds focus and blur handlers on `.copo-form-group` elements to show/hide
 * contextual help for form inputs.  For elements inside a `.rendered-control`
 * the help is injected into the `.message-segment`; otherwise a webuiPopover is used.
 * Help can be suppressed per-element by setting `data-helptip="no"`.
 */
function set_inputs_help() {
  $(document).on('focus', '.copo-form-group', function (event) {
    var elem = $(this);

    if (elem.attr('data-helptip') == 'no') {
      //help turned off
      return false;
    }

    if (!elem.find('.form-input-help').length) {
      return false;
    }

    var title = elem.find('label').html();
    if (elem.find('label').find('.constraint-label').length) {
      title = elem.find('label').clone();
      title.find('.constraint-label').remove();
      title = title.html();
    }

    var content = elem.find('.form-input-help').html();

    if (elem.closest('.rendered-control').length) {
      elem
        .closest('.rendered-control')
        .find('.message-segment')
        .html('')
        .append(
          $(
            '<div style="margin-top: 15px;" class="webpop-content-div message-segment-help ui ignored info message">' +
              content +
              '</div>'
          )
        );
    } else {
      elem.webuiPopover('destroy');

      elem.webuiPopover({
        title: title,
        content: '<div class="webpop-content-div">' + content + '</div>',
        trigger: 'sticky',
        width: 300,
        arrow: true,
        closeable: true,
        animation: 'fade',
        placement: 'right',
      });
    }
  });

  $(document).on('blur', '.copo-form-group', function (event) {
    if ($(this).closest('.rendered-control').length) {
      $(this)
        .closest('.rendered-control')
        .find('.message-segment-help')
        .remove();
    } else {
      $(this).webuiPopover('destroy');
    }
  });
}

// ═══ DIALOG AND UI TEMPLATE FACTORIES ════════════════════════════════════════

/**
 * Configures and opens a BootstrapDialog with a custom icon, title, and message.
 * Applies the `.spreadsheet-modal` class and renders the title above the message
 * body rather than in the standard modal header.
 *
 * @param {BootstrapDialog} dialog   - A pre-created BootstrapDialog instance to configure.
 * @param {string}          dTitle   - The dialog title string.
 * @param {string}          dMessage - The dialog body HTML.
 * @param {string}          dType    - One of `'warning'`, `'danger'`, or `'info'`.
 */
function dialog_display(dialog, dTitle, dMessage, dType) {
  var dTypeObject = {
    warning:
      '<div class="circular ui large basic red icon button"><i class="large icon remove"></i></div>',
    danger:
      '<div class=" ui  basic red icon button"><i class=" icon remove"></i></div>',
    info: 'fa fa-exclamation-circle copo-icon-info',
  };

  var dTypeClass = 'fa fa-exclamation-circle copo-icon-default';

  if (dTypeObject.hasOwnProperty(dType)) {
    dTypeClass = dTypeObject[dType];
  }

  var iconElement = $(dTypeClass);

  var messageDiv = $('<div/>', {
    html: dMessage,
  });

  var $dialogContent = $('<div></div>');
  $dialogContent.append($('<div/>').append(iconElement));
  $dialogContent.append(
    '<div class="copo-custom-modal-message">' + messageDiv.html() + '</div>'
  );
  dialog.realize();
  dialog.getModal().addClass('spreadsheet-modal');
  dialog.setClosable(false);
  dialog.setSize(BootstrapDialog.SIZE_SMALL);
  dialog.getModalHeader().hide();
  dialog.setTitle(dTitle);
  dialog.setMessage($dialogContent);
  dialog
    .getModalBody()
    .prepend(
      '<div class="copo-custom-modal-title">' + dialog.getTitle() + '</div>'
    );
  dialog.getModalBody().addClass('copo-custom-modal-body');
  //dialog.getModalContent().css('border', '4px solid rgba(255, 255, 255, 0.3)');
  dialog.open();
}

/**
 * Builds and returns a cloned Semantic UI split-button dropdown element used
 * as the primary actions menu on component cards and bundle panels.
 *
 * @returns {jQuery} A cloned jQuery element for the actions dropdown menu.
 */
function get_menu_control() {
  let menu_control = $(
    '<div class="copo-actions-dropdown ui dropdown">' +
      '<div class="ui active buttons">' +
      '<div class="ui big blue button menu-action-label"></div>' +
      '<div class="ui big basic blue floating dropdown icon button">' +
      '<i class="dropdown icon"></i>' +
      '</div>' +
      '</div>' +
      '<div class="text" style="color: #35637e; padding-left: 5px;"></div>' +
      '<i data-html="Please note that some items in the menu may be unavailable depending on the status"' +
      'class="info circle grey icon copo-tooltip"></i>' +
      '<div class="menu component-menu">' +
      '</div>' +
      '</div>'
  );

  return menu_control.clone();
}

/**
 * Builds and returns a cloned description-bundle card panel with a task-menu
 * dropdown, status icon, and a body placeholder (`.pbody`).
 * Used by the datafile description workflow.
 *
 * @returns {jQuery} A cloned jQuery element for the bundle panel.
 */
function get_description_bundle_panel() {
  let panel = $(
    '<div class="description-bundle-template">\n' +
      '        <div class="panel panel-dtables3">\n' +
      '            <div class="panel-heading" style="background-image: none; border: none;">\n' +
      '                <div class="bundle-header bundlename" style="font-weight: bold; font-size: 15px;"></div>\n' +
      '                <div class="attr-placeholder" style="font-weight: normal; font-size: 12px; color: #ecf2f9;"> <span class="attr-placeholder-key"> </span> <span class="attr-placeholder-value"></span></div>\n' +
      '                <div style="font-weight: normal; font-size: 12px; color: #ecf2f9;">Last modified: <span class="bundle-modified-date"></span></div>\n' +
      '            </div>\n' +
      '            <div class="panel-body">\n' +
      '                <div class="row" style="margin-bottom: 20px;">\n' +
      '                    <div class="col-sm-10 col-md-10 col-lg-10">\n' +
      '                        <div class="copo-actions-dropdown ui dropdown">\n' +
      '                            <div class="ui active buttons">\n' +
      '                                <div class="ui blue button bundle-action-label">Tasks Menu</div>\n' +
      '                                <div class="ui basic blue floating dropdown icon button">\n' +
      '                                    <i class="dropdown icon"></i>\n' +
      '                                </div>\n' +
      '                            </div>\n' +
      '                            <div class="text" style="color: #35637e;"></div>\n' +
      '                            <i data-html="Please note that some items in the menu may be unavailable depending on the status."\n' +
      '                               class="info circle grey icon copo-tooltip"></i>\n' +
      '                            <div class="menu component-menu">\n' +
      '                                <div data-task="add_datafiles" class="item bundlemenu">Add datafiles</div>\n' +
      '                                <div data-task="view_datafiles" class="item bundlemenu">Show datafiles</div>\n' +
      '                                <div class="divider"></div>\n' +
      '                                <div data-task="add_metadata" class="item bundlemenu">Add/Edit metadata</div>\n' +
      '                                <div data-task="view_metadata" class="item bundlemenu">Show metadata</div>\n' +
      '                                <div class="divider"></div>\n' +
      '                                <div data-task="edit_bundle" class="item bundlemenu">Rename bundle</div>\n' +
      '                                <div data-task="clone_bundle" class="item bundlemenu">Clone bundle</div>\n' +
      '                                <div data-task="delete_bundle" class="item bundlemenu">Delete bundle</div>\n' +
      '                                <div class="divider"></div>\n' +
      // '                                <div data-task="submit_bundle" class="item bundlemenu">Submit bundle</div>\n' +
      '                                <div data-task="view_accessions" class="item bundlemenu">View accessions</div>\n' +
      '                                <div data-task="view_issues" class="item bundlemenu">View issues</div>\n' +
      '                            </div>\n' +
      '                        </div>\n' +
      '                    </div>\n' +
      '                    <div class="col-sm-2 col-md-2 col-lg-2">\n' +
      '                        <div class="pull-right">\n' +
      '                            <i title="" class="big icon copo-tooltip bundle-status"></i>\n' +
      '                        </div>\n' +
      '                    </div>\n' +
      '                </div>\n' +
      '                <div class="row">\n' +
      '                    <div class="col-sm-12 col-md-12 col-lg-12">\n' +
      '                        <div class="pbody"></div>\n' +
      '                    </div>\n' +
      '                </div>\n' +
      '            </div>\n' +
      '        </div>\n' +
      '    </div>'
  );

  return panel.clone();
}

/**
 * Builds and returns a cloned generic component card panel with a task-menu
 * dropdown, optional attribute placeholder, status icon, and body (`.pbody`).
 *
 * @returns {jQuery} A cloned jQuery element for the card panel.
 */
function get_card_panel() {
  let panel = $(
    '<div class="component-type-panel">\n' +
      '        <div class="panel panel-dtables3">\n' +
      '            <div class="panel-heading" style="background-image: none; border: none;">\n' +
      '                <div class="panel-header-1" style="font-weight: bold; font-size: 15px;"></div>\n' +
      '                <div class="attr-placeholder" style="font-weight: normal; font-size: 12px; color: #ecf2f9; display: none;"><span class="attr-key copo-right-spacer"></span> <span class="attr-value"></span></div>\n' +
      '            </div>\n' +
      '            <div class="panel-body">\n' +
      '                <div class="row" style="margin-bottom: 20px;">\n' +
      '                    <div class="col-sm-10 col-md-10 col-lg-10">\n' +
      '                        <div class="copo-actions-dropdown ui dropdown">\n' +
      '                            <div class="ui active buttons">\n' +
      '                                <div class="ui blue button menu-label">Tasks Menu</div>\n' +
      '                                <div class="ui basic blue floating dropdown icon button menu-label-icon">\n' +
      '                                    <i class="dropdown icon"></i>\n' +
      '                                </div>\n' +
      '                            </div>\n' +
      '                            <div class="text" style="color: #35637e;"></div>\n' +
      '                            <i data-html="Please note that some items in the menu may be unavailable depending on the status."\n' +
      '                               class="info circle grey icon copo-tooltip"></i>\n' +
      '                            <div class="menu component-menu"> </div>\n' +
      '                        </div>\n' +
      '                    </div>\n' +
      '                    <div class="col-sm-2 col-md-2 col-lg-2">\n' +
      '                        <div class="pull-right">\n' +
      '                            <i title="" class="big icon copo-tooltip bundle-status"></i>\n' +
      '                        </div>\n' +
      '                    </div>\n' +
      '                </div>\n' +
      '                <div class="row">\n' +
      '                    <div class="col-sm-12 col-md-12 col-lg-12">\n' +
      '                        <div class="pbody"></div>\n' +
      '                    </div>\n' +
      '                </div>\n' +
      '            </div>\n' +
      '        </div>\n' +
      '    </div>'
  );

  return panel.clone();
}

/**
 * Returns a cloned dismissible Bootstrap alert element (`.copo-alert-message`)
 * with a close button and an inner `.alert-message` span.
 *
 * @returns {jQuery} A cloned jQuery alert element.
 */
function get_alert_control() {
  let alert = $(
    '<div class="alert alert-success alert-dismissible fade in copo-alert-message" style="background-image: none; border: none;" role="alert">\n' +
      '<button type="button" class="close" data-dismiss="alert" aria-label="Close">\n' +
      '<span aria-hidden="true">&times;</span>\n' +
      '</button>\n' +
      '<span class="webpop-content-div alert-message"></span>\n' +
      '</div>'
  );

  return alert.clone();
}

/**
 * Returns a cloned non-dismissible Bootstrap alert element (`.copo-alert-message`)
 * with an inner `.alert-message` span but no close button.
 *
 * @returns {jQuery} A cloned jQuery alert element.
 */
function get_alert_control_no_close() {
  let alert = $(
    '<div class="alert alert-success alert-dismissable fade in copo-alert-message" style="background-image: none; border: none;">\n' +
      '            <span class="webpop-content-div alert-message"></span>\n' +
      '        </div>'
  );

  return alert.clone();
}

/**
 * Returns a cloned inline image loader element (an animated GIF `loading.gif`)
 * wrapped in an `.input-group` span, suitable for embedding next to input fields.
 *
 * @returns {jQuery} A cloned jQuery loader element.
 */
function get_ajax_loader() {
  let loader = $(
    '<span class="input-group">\n' +
      '                    <img style="height: 24px; margin-left:5px;" src="/static/assets/img/loading.gif"></span>'
  );

  return loader.clone();
}

/**
 * Builds and returns a cloned collapsible panel structure used throughout COPO
 * component pages (`.panel-dtables` variant with `.copo-details-coll` collapse region
 * and a plus/minus toggle icon).
 *
 * Note: this is a different structure from the `get_collapsible_panel(panelType)`
 * overload above — they are distinct template factories.
 *
 * @returns {jQuery} A cloned jQuery element containing the panel.
 */
function get_collapsible_panel() {
  let panel = $(
    '<div class="panel-group" style="margin-top: 15px;">\n' +
      '        <div class="panel panel-dtables">\n' +
      '            <div class="panel-heading" style="background-image: none; border: none;">\n' +
      '                <h4 class="panel-title">\n' +
      '                    <div class="row">\n' +
      '                        <div class="col-sm-8 col-md-8 col-lg-8 copo-details-header"></div>\n' +
      '                        <div class="col-sm-2 col-md-2 col-lg-2 pull-right">\n' +
      '                            <div class="pull-right">\n' +
      '                                <i data-toggle="collapse" href="" class="fa fa-plus text-primary copo-details-icon"\n' +
      '                                   style="cursor: pointer; display: block;"\n' +
      '                                   aria-hidden="true"></i>\n' +
      '                            </div>\n' +
      '                        </div>\n' +
      '                    </div>\n' +
      '                </h4>\n' +
      '            </div>\n' +
      '            <div id="" class="panel-collapse collapse copo-details-coll">\n' +
      '                <div class="panel-body pbody"></div>\n' +
      '            </div>\n' +
      '        </div>\n' +
      '    </div>'
  );

  return panel.clone();
}

// ═══ COMPONENT DROPDOWN MENU ══════════════════════════════════════════════════

/**
 * Initialises interactive behaviour for component navigation dropdown menus.
 *
 * - Closes any open dropdown when the user clicks outside a wrapper.
 * - Toggles individual dropdown menus on wrapper click, moving button-driven
 *   menus to `<body>` to escape `overflow: hidden` containers, and positioning
 *   them below the trigger button.
 * - Repositions visible dropdown menus on window scroll or resize.
 * - Initialises Select2 on `.searchable-select` elements and renders options
 *   including ones that start with `●` which is a red dot/circle/bullet symbol 
 *   to indicate uploaded data.
 */
function renderOption(data) {
  if (!data || !data.id || !data.text) return data.text || ''; // placeholder or empty
  // Check if any of the dropdown menu options have uploaded data (as indicated with a prefix circle (●))
  // If yes, add a 'has-data-indicator' class to the option
  const $container = $('<span>');
  const hasData = $(data.element).data('has-data');

  if (hasData) {
    $container.append(
      $('<span>', {
        class: 'has-data-indicator',
        title: 'Has uploaded data',
        'aria-hidden': 'true',
      })
    );
  }

  $container.append(document.createTextNode(data.text));
  return $container;
}

function initialiseComponentDropdownMenu() {
  // Close component dropdowns when clicking outside
  $(document)
    .off('click.dropdownClose')
    .on('click.dropdownClose', function () {
      $('.profile-dropdown-menu.visible')
        .removeClass('visible')
        .addClass('hidden')
        .hide();
    });

  // Click handler for each dropdown wrapper
  $('.profile-dropdown-wrapper')
    .off('click.dropdown')
    .on('click.dropdown', function (e) {
      e.stopPropagation();

      const $wrapper = $(this);
      const $menu = $wrapper.find('.profile-dropdown-menu').first();
      const $button = $wrapper
        .find('.pcomponent-button, .dropdown-button, .ui.button')
        .first();

      // Hide other open dropdown menus
      $('.profile-dropdown-menu')
        .not($menu)
        .each(function () {
          const $otherMenu = $(this);
          // Return to original wrapper
          const $originalWrapper = $otherMenu.data('original-parent');
          if ($originalWrapper && $originalWrapper.length) {
            $originalWrapper.append($otherMenu);
          }
          $otherMenu.removeClass('visible').addClass('hidden').hide();
        });

      // Toggle visibility of current menu
      if ($menu.hasClass('visible')) {
        $menu.removeClass('visible').addClass('hidden').hide();
        return;
      }

      // Store original parent
      if (!$menu.data('original-parent')) {
        $menu.data('original-parent', $menu.parent());
      }

      // Component icon dropdown menu only
      if (!$button.length) {
        // Stay within wrapper — no body move, no positioning
        $menu.removeClass('hidden').addClass('visible').show();
        return;
      }

      // Component button dropdown menu only
      // Move menu to body to escape 'overflow hidden' of the components' div
      if (!$menu.parent().is('body')) {
        $menu.appendTo('body');
      }

      // Position the dropdown contents below its trigger button
      const rect = $button[0].getBoundingClientRect(); // Get button co-ordinates
      $menu.css({
        top: rect.bottom + window.scrollY + 4 + 'px',
        left: rect.left + window.scrollX + 'px',
      });
      $menu.removeClass('hidden').addClass('visible').show(); // Show menu
    });

  // Reposition visible dropdown menu on window scroll or resize
  $(window)
    .off('scroll.dropdown resize.dropdown')
    .on('scroll.dropdown resize.dropdown', function () {
      const $menu = $('.profile-dropdown-menu.visible');
      if (!$menu.length) return;

      const $wrapper = $menu.data('original-parent');
      if (!$wrapper || !$wrapper.length) return;

      const $button = $wrapper
        .find('.pcomponent-button, .dropdown-button, .ui.button')
        .first();

      // Skip repositioning if no component button is found
      // i.e. skip for icon-only components
      if (!$button.length) return;

      const rect = $button[0].getBoundingClientRect();
      $menu.css({
        top: rect.bottom + window.scrollY + 4 + 'px',
        left: rect.left + window.scrollX + 'px',
      });
    });

  // Initialise Bootstrap-select, a searchable dropdown menu
  // Select the first non-empty value option by default
  const $selectOption = $('.searchable-select');
  $selectOption.select2({
    placeholder: 'Choose an option',
    allowClear: false,
    templateSelection: renderOption,
    templateResult: renderOption,
    escapeMarkup: function (markup) {
      return markup; // Allow HTML elements
    },
  });

  const $firstOption = $('.searchable-select option')
    .filter(function () {
      return $(this).val() !== '';
    })
    .first();

  if ($firstOption.length) {
    $selectOption.val($firstOption.val()).trigger('change');
  }
}

/**
 * Initialises the responsive navigation bar toggle behaviour.
 * Handles mobile hamburger toggle, dropdown open/close on click,
 * navigation link clicks that collapse the menu on small screens, and
 * resetting the menu state when the viewport widens past 768px.
 */
function initialiseNavToggle() {
  // Initialise navigation bar on each component page
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

// ═══ MODAL / DIALOG UTILITIES ═════════════════════════════════════════════════

/**
 * Conditionally shows a BootstrapDialog close-confirmation prompt before
 * dismissing a modal.  The confirmation is only shown when a visible, non-empty
 * alert is present in the modal AND the Finish/Submit button is still enabled
 * (indicating an in-progress operation that would be lost).
 *
 * Accepts three trigger forms:
 * 1. A DOM event (the closest `.modal` ancestor is resolved as the target).
 * 2. A jQuery modal reference (`.modal('hide')` is called to close).
 * 3. A BootstrapDialog instance (`.close()` is called).
 *
 * @param {jQuery.Event|jQuery|BootstrapDialog} triggerDialogOrEvent - The trigger
 *   that initiated the close request.
 */
function confirmCloseDialog(triggerDialogOrEvent) {
  function getTargetDialog(trigger) {
    if (!trigger) return null;

    // Case 1: Modal triggered from a DOM event
    if (trigger.target) {
      const modalEl = $(trigger.target).closest('.modal');
      return modalEl.length ? modalEl : null;
    }

    // Case 2: Modal triggered with a jQuery modal reference
    if (trigger instanceof jQuery) {
      return trigger;
    }

    // Case 3: Modal triggered with a BootstrapDialog instance
    if (typeof trigger.close === 'function') {
      return trigger;
    }

    return null;
  }

  function closeDialog(dialog) {
    if (!dialog) return;

    // Close the target dialog modal
    if (dialog.close) {
      // BootstrapDialog
      dialog.close();
    } else if (dialog.modal) {
      // jQuery modal
      dialog.modal('hide');
    }
  }

  // Handle event case
  if (triggerDialogOrEvent && triggerDialogOrEvent.preventDefault) {
    triggerDialogOrEvent.preventDefault();
    triggerDialogOrEvent.stopPropagation();
  }

  // If alert is visible, 'Submit' button is disabled 
  // or 'Finish' button is disabled, skip confirmation
  const $modalAlert = $('.modal .alert, .modal .sample-alert');
  const isModalAlertVisible =
     $modalAlert.is(':visible') &&
     $modalAlert.text().trim() !== '';
  
  const isFinishButtonDisabled = $(
    '.modal-footer .btn-finish, .modal-footer .btn-submit'
  ).is(':disabled');
  const targetDialog = getTargetDialog(triggerDialogOrEvent);
  
  if (!isModalAlertVisible || isFinishButtonDisabled) {
    // Skip confirmation
    closeDialog(targetDialog);
    return;
  } else {
    // Show confirmation dialog
    BootstrapDialog.show({
      title: '<strong>Confirm close</strong>',
      message:
        'Are you sure that you would like to close the modal? ' +
        'Any upload progress will be lost.',
      cssClass: 'copo-modal1',
      closable: false,
      animate: false,
      closeByBackdrop: false, // Prevent dialog from closing by clicking on backdrop
      closeByKeyboard: false, // Prevent dialog from closing by pressing ESC key
      type: BootstrapDialog.TYPE_WARNING,
      buttons: [
        {
          label: 'No, keep open',
          cssClass: 'custom-btn tiny btn-default',
          action: function (dialogRef) {
            dialogRef.close();
          },
        },
        {
          label: 'Yes, close',
          cssClass: 'custom-btn tiny btn-primary',
          action: function (confirmDialogRef) {
            closeDialog(targetDialog);
            resetValues(); // Reset modal values
            confirmDialogRef.close(); // Close the confirmation modal
          },
        },
      ],
      onshown: function (dialogRef) {
        // Remove aria-hidden before focusing the modal
        dialogRef.getModal().removeAttr('aria-hidden');

        // Set focus after a short delay
        setTimeout(function () {
          dialogRef.getModal().focus();
        }, 50);
      },
    });
  }
}

/**
 * Fades out any `.warning-content` elements in the current modal and updates
 * the `.info-content .info-text` copy after a manifest validation response is
 * received.  Only triggers when `action` maps to a known `alertClassMap` key
 * and `message` is a non-empty, non-loading string.
 *
 * @param {string} message - The response message from the server.
 * @param {string} action  - The action key to look up in `alertClassMap`.
 */
function hideModalInstructionText(message, action) {
  // Relevant actions that trigger info update
  const shouldFade =
    Object.keys(alertClassMap).includes(String(action).toLowerCase()) &&
    Boolean(message) &&
    !message.toLowerCase().includes('loading');

  if (!shouldFade) return;

  // Fade out warning messages
  $('.warning-content').fadeOut(50);

  // Update info text if there’s a message that’s not a loading message
  $('.info-content .info-text').text('Manifest validation completed for:');
}

/**
 * Resets modal form state to its initial empty condition.
 * Clears file inputs, empties and hides alert elements, and resets
 * the tab navigation and tab content areas.  Called after a modal is closed
 * via `confirmCloseDialog`.
 */
function resetValues() {
  // Clear previous data in the modal
  $('#file, #fileid').val('');
  $('#singlecell_info').empty().hide();
  $('.warning-content').show();
  $(
    '.modal .alert.alert-info',
    '.modal .alert.alert-warning',
    '.modal .alert.alert-danger',
    '.modal .alert.alert-success'
  )
    .empty()
    .hide();

  $('.modal .nav-tabs').empty();
  $('.modal .tab-content').empty();
}

/**
 * Moves all children of `#componentInfoContainer` into the sidebar info tab
 * body (`#copo-sidebar-info .panel-body`), then removes the now-empty container.
 * Each migrated alert is enhanced with a close button, a `fade` class, and a
 * MutationObserver that syncs the `in` class with the element's display state
 * so Bootstrap fade transitions work correctly.
 */
function moveComponentInfoToTabContent() {
  // Ensure that component info alerts are displayed with
  // the other alerts in the sidebar info tab
  const $container = $('#componentInfoContainer');
  const $tab = $('#copo-sidebar-info .panel-body');

  if (!$container.length || !$tab.length) return;

  // Move all children of the block into the tab
  const $children = $container.children().appendTo($tab);

  // Remove the now-empty container
  $container.remove();

  $children.each(function () {
    const $child = $(this);
    // Add dismissible class if missing
    if (!$child.hasClass('alert-dismissible')) {
      $child.addClass('alert-dismissible fade');
    }

    // Add close button if missing
    if (!$child.find('.close').length) {
      const $closeBtn = $('<button>', {
        type: 'button',
        class: 'close',
        'aria-label': 'Close',
        html: '<span aria-hidden="true">&times;</span>',
      });

      $closeBtn.on('click', function (e) {
        // Prevent Bootstrap default removal
        e.preventDefault();
        e.stopImmediatePropagation();
        // Empty content then hide alert
        $child.find('.alert-message').empty();
        $child.hide().removeClass('in');
      });
      $child.prepend($closeBtn);
    }

    // Wrap text nodes in alert-message span
    $child
      .contents()
      .filter(function () {
        return this.nodeType === 3; // text nodes
      })
      .wrap('<span class="alert-message"></span>');
    
    // Observe style changes to add/remove 'in' class for fade effect
    const observer = new MutationObserver((mutationsList) => {
      mutationsList.forEach((mutation) => {
        if (mutation.attributeName === 'style') {
          const display = $child.css('display');
          if (display !== 'none') {
            $child.addClass('in');
          } else {
            $child.removeClass('in');
          }
        }
      });
    });

    // $child.data('inClassObserverInstance', observer);
    observer.observe($child[0], {
      attributes: true,
      attributeFilter: ['style'],
    });
  });
}
