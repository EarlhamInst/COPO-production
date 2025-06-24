$(document).ready(function () {
  initialiseOverlays(); // Initialise dropdown menus and tooltips
  // Prevent date conflicts with other scripts that
  // may have datepicker defined
  $.fn.datepicker.noConflict();

  $(document).data('manifestType', '');

  $(document).on('click', '#downloadBtn', generateManifestTemplate);

  $(document).on('click', '#resetBtn', resetManifestWizard);

  // Show info popup dialog when info icon is clicked
  $(document).on('click', '#info', function () {
    displayInfoModal(document);
  });

  // Show popup dialog when close icon is clicked
  $(document).on('click', '#closeModalIcon', displayCloseModal);

  // Update manifest version when a manifest options is selected
  $(document).on('click', '.manifest-options-menu a', function () {
    // Get manifest options' parent cell ID
    const cellId = $(this)
      .closest('.dropdown')
      .attr('id')
      .replace('ManifestDropdownDiv', '');

    const groupType = camelToSnake(cellId);
    populateManifestTable({ groupType: groupType });
  });

  handleThemeColourChange();
  wizardHandler();
  populateManifestTable();
  filterTable(document);
});

function initialiseOverlays() {
  $('.dropdown-toggle').dropdown();
  $('[data-toggle="tooltip"]').tooltip();
}

function buildDropdown({
  cellId,
  templateSelector,
  menuSelector,
  cellSuffix,
  dropdownIdSuffix,
  buttonIdSuffix,
  list,
  onItemRender,
  onItemClick,
  includeSeparators = false,
  getGroupKey,
}) {
  const $template = $(templateSelector);
  const $cell = $(`#${cellId}${cellSuffix}`);
  if ($cell.length === 0) return;

  const $dropdown = $template.clone().removeClass('hidden');
  $dropdown.attr('id', `${cellId}${dropdownIdSuffix}`);

  const $button = $dropdown.find('button');
  const buttonId = `${cellId}${buttonIdSuffix}`;
  $button.attr('id', buttonId);

  const $menu = $dropdown.find(menuSelector).empty();
  $menu.attr('aria-labelledby', buttonId);

  list.forEach((item, index) => {
    const $li = $('<li>');
    const $a = onItemRender($button, item, index);
    $li.append($a);
    $menu.append($li);

    if (
      includeSeparators &&
      index < list.length - 1 &&
      getGroupKey &&
      getGroupKey(item) !== getGroupKey(list[index + 1])
    ) {
      $menu.append($('<li role="separator">').addClass('strong-divider'));
    } else if (includeSeparators && index < list.length - 1) {
      $menu.append($('<li role="separator">').addClass('divider'));
    }
  });

  if (onItemClick) {
    $menu.find('a').on('click', function (e) {
      e.preventDefault();
      onItemClick.call(this, $button, $menu, $(this).text().trim());
    });
  }

  $cell.empty().append($dropdown);
}

function loadManifestOptions(cellId, manifestList) {
  buildDropdown({
    cellId,
    templateSelector: '#manifestDropdownTemplate',
    menuSelector: '.manifest-options-menu',
    cellSuffix: 'ManifestOptions',
    dropdownIdSuffix: 'ManifestDropdownDiv',
    buttonIdSuffix: 'ManifestDropdownBtn',
    list: manifestList,
    includeSeparators: true,
    getGroupKey: (item) => item.technology || item.type.split(' ')[0],
    onItemRender: ($button, item, index) => {
      const $a = $('<a>', {
        href: '#',
        text: item.label || item.name,
        title: item.description || '',
      });

      if (item.description) {
        $a.attr({
          'data-toggle': 'tooltip',
          'data-placement': 'right',
          'data-html': 'true',
          'data-original-title': item.description,
        });
      }

      if (index === 0) {
        const label = $a.text().trim();
        $a.addClass('active').attr('data-selected', 'true');
        $button.html(`
          <span class="dropdown-label">${label}</span>
          <span class="caret"></span>
        `);
      }

      return $a;
    },
    onItemClick: function ($button, $menu, label) {
      $button.html(`
        <span class="dropdown-label">${label}</span>
        <span class="caret"></span>
      `);
      $menu.find('a').removeClass('active').removeAttr('data-selected');
      $(this).addClass('active').attr('data-selected', 'true');
      $button.dropdown('toggle');
    },
  });
}

function setSelectedItemManifestVersion(cellId, manifestList) {
  const $versionCell = $(`#${cellId}ManifestVersion`);
  if ($versionCell.length === 0) return;

  const $selectedOption = $(
    `#${cellId}ManifestOptions .manifest-options-menu a[data-selected="true"]`
  );
  if ($selectedOption.length === 0) return;

  const selectedLabel = $selectedOption.text().trim();
  const selectedItem = manifestList.find(
    (item) => (item.label || item.name) === selectedLabel
  );

  if (!selectedItem) return;

  $versionCell.text(selectedItem.version || '-');
}

function loadDownloadOptions(cellId, manifestList) {
  const $selectedManifestOption = $(
    `#${cellId}ManifestOptions .manifest-options-menu a[data-selected="true"]`
  );

  const selectedItem = manifestList.find(
    (item) =>
      (item.label || item.name) === $selectedManifestOption.text().trim()
  );
  if (!selectedItem) return;

  const downloadItems = [];

  if (selectedItem.blank_manifest_url) {
    downloadItems.push({
      label: 'Blank template',
      className: 'blank-template-link',
      href: selectedItem.blank_manifest_url,
      download: selectedItem.file_name,
      title: `Download blank template for ${selectedItem.name}`,
      click: function (e) {
        e.preventDefault();
        downloadFile({
          url: selectedItem.blank_manifest_url,
          filename: selectedItem.file_name,
        });
      },
    });
  }

  if (selectedItem.sop_url) {
    downloadItems.push({
      label: 'Standard Operating Procedure (SOP)',
      className: 'sop-link',
      href: selectedItem.sop_url,
      target: '_blank',
      title: `View SOP for ${selectedItem.name}`,
    });
  }

  if (selectedItem.show_prefill_wizard) {
    downloadItems.push({
      label: 'Launch Prefilled Wizard',
      className: 'prefilled-template-option',
      href: '#',
      title: `Launch wizard for ${selectedItem.name} samples`,
      click: function (e) {
        e.preventDefault();
        showWizardBasedOnManifestType(selectedItem.name);
      },
    });
  }

  buildDropdown({
    cellId,
    templateSelector: '#downloadDropdownTemplate',
    menuSelector: '.download-options-menu',
    cellSuffix: 'DownloadOptions',
    dropdownIdSuffix: 'DownloadMenu',
    buttonIdSuffix: 'DownloadBtn',
    list: downloadItems,
    includeSeparators: true,
    onItemRender: (_button, item, _index) => {
      const $a = $('<a>')
        .text(item.label)
        .addClass(item.className)
        .attr({
          href: item.href,
          title: item.title,
          ...(item.download && { download: item.download }),
          ...(item.target && { target: item.target }),
        });

      if (item.click) $a.on('click', item.click);

      return $a;
    },
  });
}

function populateManifestTable({ groupType = '' } = {}) {
  $.ajax({
    type: 'GET',
    url: 'populate_manifest_table',
    dataType: 'json',
    data: {},
  })
    .done(function (data) {
      // Group data by manifest 'type'
      const grouped = data.reduce((groups, item) => {
        (groups[item.type] = groups[item.type] || []).push(item);
        return groups;
      }, {});
      const isAllGroups = groupType === '';
      const targetGroups = isAllGroups ? Object.keys(grouped) : [groupType];

      targetGroups.forEach((type) => {
        const manifestList = grouped[type];
        const cellId = toCamelCase(type);

        // Load manifest options if no specific groupType is selected
        if (isAllGroups) {
          loadManifestOptions(cellId, manifestList);
        }

        setSelectedItemManifestVersion(cellId, manifestList);
        loadDownloadOptions(cellId, manifestList);
      });

      // Initialise dropdown menus
      initialiseOverlays();
    })
    .fail(function (error) {
      console.error(
        'Error:',
        error.message || error.statusText || error.toString()
      );
    });
}

function displayInfoModal(document) {
  let manifestType = $(document).data('manifestType').toLowerCase();

  const asg_sop_link = 'https://github.com/darwintreeoflife/metadata';
  const dtol_sop_link = asg_sop_link;
  const erga_sop_link =
    'https://github.com/ERGA-consortium/ERGA-sample-manifest';
  const dtolenv_sop_link = '';

  const sop_link =
    manifestType === 'asg'
      ? asg_sop_link
      : manifestType === 'dtol'
      ? dtol_sop_link
      : manifestType === 'erga'
      ? erga_sop_link
      : manifestType === 'dtolenv'
      ? dtolenv_sop_link
      : '';

  let $info_message =
    '<ul  style = ' +
    "'padding-top: 15px; padding-left: 10px; padding-right: 10px;" +
    "'> ";
  $info_message +=
    '<li>To input a common value, select a field name from the dropdown list</li> ';
  $info_message +=
    '<li style = ' +
    "'margin-top: 10px;" +
    "'>Inputted values <b>must</b> conform to the ";
  $info_message +=
    '<a href=' + sop_link + '>Standard Operating Procedure (SOP)</a>';
  $info_message += '</li>';
  $info_message += '</ul>';

  bootbox.dialog({
    size: 'small',
    message: $info_message,
    buttons: {
      success: {
        label: 'OK',
        className: 'btn-sm btn-primary okbtn',
      },
    },
  });
}

function displayCloseModal() {
  bootbox.confirm({
    message:
      'Are you sure that you would like to close the dialog? All inputted values will be lost.',
    buttons: {
      confirm: {
        label: '<i class="fa fa-check"></i> Yes, close dialog',
      },
      cancel: {
        label: '<i class="fa fa-times"></i> Cancel',
      },
    },
    callback: function (result) {
      if (result) {
        resetManifestWizard(); // Reset values in the wizard
        $('#modal-placeholder').modal('hide');
      }
    },
  });
}
// Set text colour according to the current theme
function handleThemeColourChange() {
  let section_header = $('.secondary-container').find('p');
  let manifest_filter_div = $('.manifest-filter-div');
  let manifest_filter_icon = $('.manifest-filter-icon');

  if ($('.grey-theme').is(':visible')) {
    section_header.addClass('white-text');

    if (manifest_filter_div.is(':visible')) {
      manifest_filter_icon.toggleClass('white-text');
    }
  } else {
    section_header.removeClass('white-text');
    if (manifest_filter_div.is(':visible')) {
      manifest_filter_icon.toggleClass('black-text');
    }
  }
}

function filterTable(document) {
  document
    .getElementById('manifestFilter')
    .addEventListener('keyup', function () {
      const filter = this.value.toLowerCase();
      const rows = document.querySelectorAll('#manifestTable tbody tr');

      rows.forEach((row) => {
        let match = false;
        const cells = Array.from(row.cells);

        // Skip the last cell by slicing off the last one
        // as it contains the 'Download' dropdown menu which
        // should not be filtered
        cells.slice(0, -1).forEach((cell) => {
          // Check if the cell has a <select> tag
          const select = cell.querySelector('select');

          if (select) {
            // Check all dropdown options
            Array.from(select.options).forEach((option) => {
              if (option.text.toLowerCase().includes(filter)) {
                match = true;
              }
            });
          } else {
            // Check plain text
            if (cell.textContent.toLowerCase().includes(filter)) {
              match = true;
            }
          }
        });

        // Show or hide the row
        row.style.display = match ? '' : 'none';
      });
    });
}

function wizardHandler() {
  $('#manifest-wizard')
    .on('change.fu.wizard', function () {
      // console.log('change');
    })
    .on('changed.fu.wizard', function () {
      let currentStep = $('#manifest-wizard').wizard('selectedItem').step;
      let prevButton = $('.btn-prev');
      let resetButton = $('.btn-reset');
      let rightIcon = $('#rightIcon');

      // Reveal/show the next icon/previous button/reset button
      // from the final step of the wizard
      currentStep === 2 ? prevButton.show() : prevButton.hide();
      currentStep === 2 ? resetButton.hide() : resetButton.show();
      currentStep === 2 ? rightIcon.hide() : rightIcon.show();
    })
    .on('finished.fu.wizard', function (e) {
      // console.log('finished');
      $('#modal-placeholder').modal('hide');
      $('#spinner_div').removeClass('hidden'); // Show spinner div
      generateManifestTemplate(e);
    })
    .on('stepclick.fu.wizard', function (e, data) {
      //  console.log('Step' + data.step + ' clicked');
    })
    .on('actionclicked.fu.wizard', function (e, data) {
      validateCommonValue(e, data);
    });
}

function resetManifestWizard() {
  let commonFieldsDropdownListDivID = document.querySelector(
    '#commonFieldsDropdownListDiv'
  );
  let commonFieldErrorMessageID = document.getElementById(
    'commonFieldErrorMessageID'
  );
  document.getElementById('numberOfSamples').value = 1; // Preload with default number of samples

  $('#formID .form-group').remove(); // Remove/clear all existing divs from the form
  $('.btn-prev').hide(); // Hide previous button

  // Remove error information if it is shown
  if (commonFieldsDropdownListDivID.classList.contains('has-error')) {
    commonFieldsDropdownListDivID.classList.remove('has-error');
    commonFieldErrorMessageID.innerHTML = '';
    commonFieldErrorMessageID.style.display = 'none';
  }

  buildCommonFields(); // Preload with the common fields dropdown list
}

function buildCommonFields() {
  // Get fields from the manifest schema based on the manifest type
  // const manifestType = document.querySelector('#manifestType').value;
  let manifestType = $(document).data('manifestType');

  $.ajax({
    type: 'GET',
    url: '/manifests/get_manifest_fields/',
    dataType: 'json',
    data: {
      manifestType: manifestType,
    },
  })
    .done(function (data) {
      let commonFieldsList = $('#commonFields');
      let option = [];
      let colour_lst = [];

      // Add a default value to the dropdown list
      commonFieldsList.empty();
      commonFieldsList.append(
        '<option selected disabled hidden value=""' +
          '>' +
          'Choose a common field' +
          '</option>'
      );

      for (let i = 0; i < data.length; i++) {
        let [order_num, excel_column_letter, current_colour, fieldname] =
          data[i];
        option = fieldname;

        let field_option = $('<option/>', {
          style:
            'background-color: ' +
            current_colour +
            '  !important;padding: 10px 5px',
          value: option,
        });

        // Add a divider/separator between the groups of colours
        let [
          prev_order_num,
          prev_excel_column_letter,
          prev_colour,
          prev_fieldname,
        ] = data[i - 1] || '';

        if (prev_colour != null && current_colour != null) {
          if (prev_colour !== '' && current_colour !== '') {
            if (prev_colour !== current_colour) {
              commonFieldsList.append(
                '<option disabled>────────────────────────────────────</option>'
              );
            }
          }
          // Place a divider before the group of fields that have no assigned colour
          if (
            prev_colour !== '' &&
            !current_colour.includes('#') &&
            option !== ''
          ) {
            commonFieldsList.append(
              '<option disabled>────────────────────────────────────</option>'
            );
          }
        }

        // Populate the dropdown menu
        field_option.text(
          excel_column_letter + '\xa0\xa0\xa0\xa0\xa0\xa0' + option
        );
        if (option === 'RACK_OR_PLATE_ID' || option === 'TUBE_OR_WELL_ID') {
          // Value for the fields - "RACK_OR_PLATE_ID" || option == "TUBE_OR_WELL_ID"
          // should be scanned in using their barcodes not manually entered
          field_option.attr('disabled', 'disabled');
          field_option.attr(
            'title',
            'Value should be scanned in not manually entered'
          );

          commonFieldsList.append(field_option);
        } else {
          commonFieldsList.append(field_option);
        }
      }
    })
    .fail(function (error) {
      console.error(
        'Error:',
        error.message || error.statusText || error.toString()
      );
    });
}

function initCommonValueDropdown(commonField, commonValueDiv) {
  // Get dropdown list fields from manifest schema based on the common field and/manifest type
  // const manifestType = document.querySelector('#manifestType').value;
  let manifestType = $(document).data('manifestType');

  $.ajax({
    type: 'GET',
    url: '/manifests/get_common_value_dropdown_list/',
    dataType: 'json',
    data: {
      manifestType: manifestType,
      commonField: commonField,
    },
  })
    .done(function (data) {
      if (data['dropdown_list'].length !== 0) {
        const value_input = document.createElement('select');
        value_input.setAttribute('id', 'commonValueID');
        value_input.setAttribute('class', 'form-control');
        value_input.setAttribute('aria-describedby', 'commonValueStatus');
        value_input.setAttribute('required', '');
        value_input.style.width = '200px'; // Set width of the select tag field

        let option = [];

        $(value_input).empty();
        $(value_input).append(
          '<option selected disabled hidden value=""' +
            '>' +
            'Choose common value' +
            '</option>'
        );

        for (let i = 0; i < data['dropdown_list'].length; i++) {
          option = data['dropdown_list'][i];
          $(value_input).append(
            '<option value="' + option + '">' + option + '</option>'
          );
        }
        commonValueDiv.appendChild(value_input);
      } else {
        // Create input tag
        const value_input = document.createElement('input');
        value_input.setAttribute('class', 'form-control');
        value_input.setAttribute('required', '');
        value_input.setAttribute('aria-describedby', 'commonValueStatus');

        // Create date field
        if (data['date_fields'].includes(commonField)) {
          // Get date picker for common field that requires a date as its value
          // Date selected has to be before the current date i.e. a past date
          value_input.setAttribute('type', 'text');
          value_input.setAttribute('placeholder', 'Select date');
          // Date is based on class instead of ID due to jQuery and FuelUX conflicts
          // and multiple instances of the datepicker function cannot have the same ID
          $(value_input).addClass('datepicker');

          commonValueDiv.appendChild(value_input);

          // The "hasDatepicker" class triggers the datepicker function so it has to be present
          // in the input field that requires a date. The following line ensures
          // that the "hasDatepicker" class is present in each date field after the date
          // field is added to the commonValueDiv
          $('input').filter('.datepicker').datepicker({
            dateFormat: 'yy-mm-dd',
            maxDate: 0,
          });
        } else if (commonField === 'TIME_OF_COLLECTION') {
          // Create time field
          value_input.setAttribute('id', 'commonValueID');
          value_input.setAttribute('type', 'time');
          value_input.setAttribute('min', '0:00');
          value_input.setAttribute('max', '24:00');
          value_input.setAttribute('placeholder', 'Choose time');

          commonValueDiv.appendChild(value_input);
        } else if (data['integer_fields'].includes(commonField)) {
          // Create integer field
          value_input.setAttribute('id', 'commonValueID');
          value_input.setAttribute('type', 'number');
          value_input.setAttribute('placeholder', 'Enter integer');
          value_input.setAttribute(
            'onkeypress',
            'return event.keyCode === 8 || event.charCode > 48 && event.charCode <= 57'
          );
          value_input.setAttribute('min', '1');
          value_input.setAttribute('max', '10000');

          commonValueDiv.appendChild(value_input);
        } else {
          value_input.setAttribute('id', 'commonValueID');
          value_input.setAttribute('type', 'text');
          value_input.setAttribute('placeholder', 'Enter common value');
          value_input.setAttribute('value', '');

          commonValueDiv.appendChild(value_input);
        }
      }
    })
    .fail(function (error) {
      console.error(
        'Error:',
        error.message || error.statusText || error.toString()
      );
    });
}

function disableSelectedOption(commonField) {
  // Disable the selected option in dropdown list
  $('#commonFields option:contains(' + commonField + ')').attr(
    'disabled',
    'disabled'
  );
  // Revert to the default option after selected option has been disabled in the dropdown list
  $('select').prop('selectedIndex', 0);
}

function insertFormDiv(commonField) {
  const formDiv = document.getElementById('formDiv');
  // Create a form tag
  const form = document.getElementById('formID');
  $(form).addClass('form-horizontal'); // form-horizontal form-inline

  // Create common field div
  const commonFieldDiv = document.createElement('div');
  commonFieldDiv.setAttribute('class', 'form-group has-feedback');
  commonFieldDiv.setAttribute('id', `${commonField.value}_div`);

  // Error message field cell
  const error_message_field = document.createElement('textarea');
  error_message_field.setAttribute('id', 'errorMessageID');

  error_message_field.setAttribute('readonly', '');
  error_message_field.setAttribute('class', 'form-control');
  error_message_field.setAttribute('rows', '1');
  error_message_field.setAttribute('wrap', 'soft');
  error_message_field.innerHTML = '';
  error_message_field.style.overflowY = 'scroll';
  // error_message_field.style.height = '48px';
  error_message_field.style.width = '211px';
  // error_message_field.style.maxWidth = '270px';
  error_message_field.style.marginLeft = '243px';
  error_message_field.style.marginBottom = '10px';
  error_message_field.style.resize = 'none';
  error_message_field.style.display = 'none'; // Hide error message textarea tag
  commonFieldDiv.appendChild(error_message_field);

  // Common field; Create common field label
  const commonFieldLabel = document.createElement('label');
  commonFieldLabel.innerHTML = commonField.value;
  commonFieldLabel.setAttribute('class', 'cfID control-label col-sm-6');
  commonFieldLabel.setAttribute('for', 'commonValueID');
  commonFieldLabel.style.paddingRight = '20px'; // Add space between the value field and field name
  commonFieldLabel.style.marginLeft = '10px';
  commonFieldLabel.style.textAlign = 'left';

  // Truncate long field names
  commonFieldLabel.style.whiteSpace = 'nowrap';
  commonFieldLabel.style.textOverflow = 'ellipsis';
  commonFieldLabel.style.overflow = 'hidden';
  commonFieldLabel.style.maxWidth = '220px';
  commonFieldDiv.appendChild(commonFieldLabel);

  // Create common value div
  const commonValueDiv = document.createElement('div');
  commonValueDiv.setAttribute('class', 'col-sm-5 commonValueDiv');
  commonFieldDiv.appendChild(commonValueDiv);

  // Value field
  initCommonValueDropdown(commonField.value, commonValueDiv);

  // Delete icon tag
  const deleteIcon = document.createElement('i');
  deleteIcon.setAttribute('type', 'button');
  deleteIcon.setAttribute('onclick', 'removeFormDiv(this)');
  deleteIcon.setAttribute('class', 'fa fa-trash-can');
  deleteIcon.setAttribute('title', 'Remove from manifest');
  deleteIcon.style.marginTop = '7px'; // Center icon
  $(deleteIcon).css({ color: 'red' });
  commonFieldDiv.appendChild(deleteIcon);

  // Append the form to the div
  $(form).append(commonFieldDiv);
  formDiv.appendChild(form);

  // Disable selected common field in the dropdown list
  disableSelectedOption(commonField.value);

  // Make the form scrollable once it contains at least 6 divs
  let divs_in_form = document.querySelectorAll('#formID .form-group');

  if (divs_in_form.length >= 6) {
    $(formDiv).css({ overflow: 'scroll' });
    $(formDiv).css({ height: '200px' });
    // Set distance between the delete icon and scroll once form div becomes scrollable
    $(formDiv).css({ 'margin-right': '20px' });
  }
}

// noinspection JSUnusedGlobalSymbols
function removeFormDiv(div) {
  const formDiv = document.getElementById('formDiv');
  let divs_in_form = document.querySelectorAll('#formID .form-group');

  // Get the common field name from the div within the form
  let commonField = $(div).closest('div .form-group').find('.cfID').text();

  // Enable the common field name that is disabled in the dropdown list by removing the disable attribute
  $('#commonFields option:contains(' + commonField + ')').removeAttr(
    'disabled'
  );
  $(div).closest('div').remove(); // Remove the div from the form

  // Once the form is less than 6 rows, retain the initial height of the form/modal
  // by removing the css that were added to make the form tag div scrollable
  // when more than or equal to 6 rows were present in the form
  if (divs_in_form.length < 6) {
    $(formDiv).css({ overflow: '' });
    $(formDiv).css({ height: '' });
    $(formDiv).css({ 'margin-right': '' });
  }
}

// Trigger manifest wizard modal with desired manifest type
function showWizardBasedOnManifestType(manifestType) {
  $(document).data('manifestType', manifestType);
  let manifest_wizard = $('#manifest-wizard');
  $('#modal-placeholder').modal('show');
  manifest_wizard.wizard();
  // Automatically navigate to step 1 when the modal is launched
  // since step 0 is about selecting the manifest which has been done indirectly
  manifest_wizard.wizard('selectedItem', { step: 1 });
  $('#formID .form-group').remove(); // Remove/clear all existing divs from the form
  document.getElementById('numberOfSamples').value = 1; // Preload with default number of samples
  buildCommonFields(); // Preload with the common fields dropdown list
}

function validateCommonValue(e, data) {
  function validateFormDivData() {
    $('#formID .form-group').each(function () {
      let element = $(this);
      let commonField = element.find('.cfID').text();
      let error_message_tag = element.find('#errorMessageID');

      // Common value is either a text enclosed within an input tag or a date enclosed within a select tag
      let common_value =
        element.find('.commonValueDiv input').val() ??
        element.find('.commonValueDiv select').val();

      // Display an error message if the common value is undefined, null or empty
      if (common_value == null || common_value === '') {
        error_message_tag.css({ display: '' }); // Reveal hidden textarea tag to show the error message
        element.addClass('has-error');
        error_message_tag.val('Field cannot be empty!');
      } else {
        // Remove error information if it is shown
        if (element.hasClass('has-error')) {
          element.removeClass('has-error');
          error_message_tag.val('');
          error_message_tag.css({ display: 'none' });
        }
        // Use an ajax handler to validate the common value with regex expression
        // now that the common value is neither undefined, null or empty i.e. it has a value
        $.ajax({
          type: 'GET',
          url: '/manifests/validate_common_value/',
          dataType: 'json',
          data: {
            commonField: commonField,
            commonValue: common_value,
          },
        })
          .done(function (data) {
            if (data['response']) {
              // Check if any of the common value has invalid data in any of the div within the form
              // If errors exist, then, do nothing, remain on step 2 of the manifest wizard
              // else, navigate to the next step which is step 3 of the manifest wizard
              let divs_in_form_with_error_class =
                document.querySelectorAll('#formID .has-error');
              let number_of_errors_in_form =
                divs_in_form_with_error_class.length;
              number_of_errors_in_form === 0
                ? $('#manifest-wizard').wizard('selectedItem', { step: 2 })
                : e.preventDefault();
            } else {
              let validation_error_message = `Invalid value! Field must be ${data['error']}!`;
              error_message_tag.css({ display: '' }); // Reveal hidden textarea tag to show the error message
              element.addClass('has-error');
              error_message_tag.css({ height: '41px' });
              // Increase the height of the error message textarea field if the
              // error message is more than or equal to 50 characters
              validation_error_message.length >= 50
                ? error_message_tag.css({ height: '60px' })
                : error_message_tag.css({ height: '0px' });
              error_message_tag.val(validation_error_message);
              error_message_tag.attr('title', validation_error_message);
              e.preventDefault();
            }
          })
          .fail(function (error) {
            console.error(
              'Error:',
              error.message || error.statusText || error.toString()
            );
          });
      }
    });
  }

  if (data.step === 1 && data.direction === 'next') {
    e.preventDefault(); // Prevent navigating to the next step of the manifest wizard
    let divs_in_form = document.querySelectorAll('#formID .form-group');
    let commonFieldErrorMessageID = document.getElementById(
      'commonFieldErrorMessageID'
    );
    let commonFieldsDropdownListDivID = document.querySelector(
      '#commonFieldsDropdownListDiv'
    );
    let commonFieldsID = document.getElementById('commonFields');

    // Check if the value of the default/disabled common field is an empty string and
    // check if the number of divs within the form is non-existent i.e equal to zero
    if (commonFieldsID.value === '' && divs_in_form.length === 0) {
      commonFieldsDropdownListDivID.classList.add('has-error');
      commonFieldErrorMessageID.innerHTML =
        'Choose a common field then, enter or select its value before proceeding!';
      commonFieldErrorMessageID.style.display = ''; // Reveal span tag with error message
    } else {
      // Remove error information if it is shown
      if (commonFieldsDropdownListDivID.classList.contains('has-error')) {
        commonFieldsDropdownListDivID.classList.remove('has-error');
        commonFieldErrorMessageID.innerHTML = '';
        commonFieldErrorMessageID.style.display = 'none';
      }
      // Each common field and inputted/selected common value is located within a div
      // and the div is located within a form
      validateFormDivData();
    }
  }
}

function generateManifestTemplate(event) {
  // User needs to be loggedin so that the CSRF cookie is set,
  // "{% csrf_token %}" has to be included within a form tag in the manifests.html webpage
  // so that X-CSRFToken is set and HTTPS 403 Forbidden does not occur
  // XMLHttpRequest() has to be used instead of Ajax when downloading files with JavaScript
  event.preventDefault();
  const xhr = new XMLHttpRequest();
  // const manifestType = document.querySelector('#manifestType').value;
  let manifestType = $(document).data('manifestType');
  const numberOfSamples = document.getElementById('numberOfSamples').value;

  let csrftoken = $('[name="csrfmiddlewaretoken"]').val(); //.attr('value');
  let commonFieldsList = [];
  let commonValuesList = [];

  $('#formID .form-group').each(function () {
    let common_value;
    let element = $(this);
    let commonField = element.find('.cfID').text();

    // Get value from input tag or select tag
    // If the input tag is "undefined" or "null" then, the value originates from a select tag and vice versa

    if (
      typeof element.find('.commonValueDiv input').val() === 'undefined' ||
      element.find('.commonValueDiv input').val() === null
    ) {
      common_value = element.find('.commonValueDiv select').val();
    } else {
      // typeof element.find('.commonValueDiv select').val() === 'undefined' || element.find('.commonValueDiv select').val() === null)
      common_value = element.find('.commonValueDiv input').val();
    }

    //.append() cannot be used to add an item to a list/array in JavaScript so .push() is used instead
    commonFieldsList.push(commonField);
    commonValuesList.push(common_value);
  });

  // Download the manifest template and show the spinner div
  // while the manifest template is being downloaded
  $('#spinner_div').removeClass('hidden');

  getManifestFilename(manifestType)
    .then((fileName) => {
      if (!fileName) {
        console.error('Failed to retrieve manifest filename.');
        $('#spinner_div').addClass('hidden');
        return;
      }
      const payload = {
        rowCount: numberOfSamples,
        manifestType: manifestType,
        commonFieldsList: commonFieldsList,
        commonValuesList: commonValuesList,
      };

      downloadFile({
        url: '/manifests/prefill_manifest_template/',
        filename: fileName,
        method: 'POST',
        body: JSON.stringify(payload),
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrftoken,
        },
      });
    })
    .catch((error) => {
      console.error('Error retrieving manifest filename:', error);
      $('#spinner_div').addClass('hidden');
    });
}

function getManifestFilename(manifestType) {
  return fetch(
    `/manifests/get_manifest_file_name/${encodeURIComponent(manifestType)}`
  )
    .then((response) => {
      if (!response.ok) {
        console.error(`HTTP error! Status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      return data.file_name;
    })
    .catch((error) => {
      console.error('Error:', error);
      return null;
    });
}

function downloadFile({
  url,
  filename,
  method = 'GET',
  body = null,
  headers = {},
  onError = null,
}) {
  fetch(url, {
    method,
    body,
    headers,
  })
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.blob();
    })
    .then((blob) => {
      const blobUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = blobUrl;
      a.download = filename || 'download.xlsx';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(blobUrl);
      $('#spinner_div').addClass('hidden');
    })
    .catch((err) => {
      console.error('Download failed:', err);
      $('#spinner_div').addClass('hidden');
      if (typeof onError === 'function') {
        onError(err);
      } else {
        console.error('Failed to generate manifest.');
      }
    });
}

function toCamelCase(str) {
  return str.toLowerCase().replace(/[_\s]+(.)/g, (_, chr) => chr.toUpperCase());
}

function camelToSnake(str) {
  return str.replace(/([A-Z])/g, '_$1').toLowerCase();
}
