// Registering batches of samples under EDP profiles.
//
// Each batch has its own number of samples, sample type, container type and
// library type (which is only asked for pre-made libraries). A new EDP profile
// is only saved along with its first batch: the profile form's 'Save' button
// becomes 'Next', which opens the batch dialogue and the sample batch is saved with the
// profile.
// Additional batches of samples are added from the profile's options menu where
// batches can also be changed until any of their samples have been submitted.

const EDP_PROFILE_TYPE = 'ei_edp';
const SAMPLE_BATCH_OPTIONS_URL = '/copo/ei_edp/sample_batch_options/';
const BATCH_FIELD_NAMES = ['no_of_samples', 'sample_type', 'container_type', 'library_type'];

function getSampleBatchesURL(profileId) {
  return `/copo/ei_edp/${profileId}/sample_batches`;
}

function isNewEdpProfileForm(formJSON, dialogRef) {
  if (formJSON.component_name !== 'profile' || formJSON.target_id) {
    return false;
  }
  const profileType = dialogRef.getModalBody().find('[id="copo.profile.type"]').val();
  return String(profileType || '').toLowerCase().trim() === EDP_PROFILE_TYPE;
}

function isPremadeLibrary(sampleType, options) {
  const premadeSampleTypes = options.premade_library_sample_types.map((type) => type.toLowerCase());
  return premadeSampleTypes.includes(String(sampleType || '').toLowerCase().trim());
}

function buildFieldLabel(name, options) {
  // Labelled like the profile form's fields (see 'form_label_ctrl' in
  // copo_form_handlers.js): the label, 'required' flag and help tip
  // come from the profile schema
  const field = options.fields[name];
  return form_label_ctrl({
    id: `batch_${name}`,
    label: field.label,
    required: field.required,
    help_tip: field.help_tip,
  });
}

function buildSelectControl(name, options, optionValues, placeholder) {
  const $select = $('<select/>', { class: 'form-control', name: name, id: `batch_${name}` });
  $select.append($('<option/>', { value: '', text: placeholder }));
  optionValues.forEach((option) => {
    $select.append($('<option/>', { value: option.value, text: option.label }));
  });
  return $('<div/>', { class: `form-group batch-field-${name}` })
    .append(buildFieldLabel(name, options))
    .append($select);
}

function buildBatchForm(options) {
  const $form = $('<form/>', { class: 'sample-batch-form', novalidate: true });

  $form.append(
    $('<div/>', { class: 'form-group batch-field-no_of_samples' })
      .append(buildFieldLabel('no_of_samples', options))
      .append(
        $('<input/>', {
          type: 'number',
          min: 1,
          step: 1,
          class: 'form-control',
          name: 'no_of_samples',
          id: 'batch_no_of_samples',
        })
      )
  );
  $form.append(
    buildSelectControl('sample_type', options, options.sample_type_options, 'Select sample type')
  );
  $form.append(
    buildSelectControl(
      'container_type',
      options,
      options.container_type_options,
      'Select container type'
    )
  );
  $form.append(
    buildSelectControl(
      'library_type',
      options,
      options.library_type_options,
      'Select library type'
    )
  );
  // Show the help tips on hover, as the profile form does
  $form.find('.copo-tooltip').popup();

  // The library type only applies to pre-made libraries so, it is hidden otherwise
  const toggleLibraryType = () => {
    const sampleType = $form.find('[name="sample_type"]').val();
    $form.find('.batch-field-library_type').toggle(isPremadeLibrary(sampleType, options));
  };
  $form.on('change', '[name="sample_type"]', toggleLibraryType);
  $form.data('toggleLibraryType', toggleLibraryType);
  toggleLibraryType();

  return $form;
}

function setBatchFormValues($form, batch) {
  BATCH_FIELD_NAMES.forEach((name) => {
    let value = batch ? batch[name] : '';
    if (name === 'container_type' && value) {
      // Match the option values whatever their case
      const $option = $form
        .find('[name="container_type"] option')
        .filter((_, option) => option.value.toLowerCase() === String(value).toLowerCase());
      value = $option.length ? $option.val() : value;
    }
    $form.find(`[name="${name}"]`).val(value === undefined || value === null ? '' : value);
  });
  $form.data('toggleLibraryType')();
}

function getBatchFormValues($form, options) {
  const fields = {};
  BATCH_FIELD_NAMES.forEach((name) => {
    fields[name] = String($form.find(`[name="${name}"]`).val() || '').trim();
  });
  if (!isPremadeLibrary(fields.sample_type, options)) {
    fields.library_type = '';
  }
  return fields;
}

function checkBatchFormValues(fields, options) {
  // The server checks these again; this only saves a round trip
  const labels = options.labels;
  const errors = [];
  if (!/^\d+$/.test(fields.no_of_samples) || parseInt(fields.no_of_samples, 10) < 1) {
    errors.push(`<strong>${labels.no_of_samples}</strong> must be a whole number of 1 or more.`);
  }
  ['sample_type', 'container_type'].forEach((name) => {
    if (!fields[name]) {
      errors.push(`Please fill in <strong>${labels[name]}</strong>.`);
    }
  });
  if (isPremadeLibrary(fields.sample_type, options) && !fields.library_type) {
    errors.push(`Please fill in <strong>${labels.library_type}</strong>.`);
  }
  return errors;
}

function showBatchDialogMessage(dialogRef, message, status) {
  const $message = dialogRef.getModalBody().find('.sample-batch-message');
  if (!message) {
    $message.empty().hide();
    return;
  }
  const alertClass = { success: 'alert-success', warning: 'alert-warning' }[status] || 'alert-danger';
  $message.html($('<div/>', { class: `alert ${alertClass}`, role: 'alert' }).html(message)).show();
}

function getResponseMessage(xhr, fallback) {
  return (xhr && xhr.responseJSON && xhr.responseJSON.message) || fallback;
}

// --- A new EDP profile: its first batch is registered before it is saved ---

function readHiddenBatchFields($profileForm) {
  const batch = {};
  BATCH_FIELD_NAMES.forEach((name) => {
    batch[name] = $profileForm.find(`[id="copo.profile.${name}"]`).val() || '';
  });
  return batch;
}

function writeHiddenBatchFields($profileForm, fields) {
  // The profile form saves every input by its ID so, the batch is
  // saved with the profile and moved into its first batch on the server
  BATCH_FIELD_NAMES.forEach((name) => {
    let $input = $profileForm.find(`[id="copo.profile.${name}"]`);
    if (!$input.length) {
      $input = $('<input/>', { type: 'hidden', id: `copo.profile.${name}` });
      $profileForm.append($input);
    }
    $input.val(fields[name]);
  });
}

function openFirstBatchDialog(profileDialogRef, proceed) {
  const $profileForm = profileDialogRef.getModalBody().find('form');

  $.getJSON(SAMPLE_BATCH_OPTIONS_URL)
    .done(function (options) {
      const $batchForm = buildBatchForm(options);
      const previousBatch = readHiddenBatchFields($profileForm);
      setBatchFormValues($batchForm, previousBatch.no_of_samples ? previousBatch : null);

      const $content = $('<div/>')
        .append(
          $('<p/>', { class: 'alert alert-info' })
          .text(
            'Register the first batch of samples for this profile. ' +
            'The profile is created along with the samples. ' +
            "More samples can be added later from the profile's options."
          )
        )
        .append($('<div/>', { class: 'sample-batch-message' }).hide())
        .append($batchForm);

      BootstrapDialog.show({
        title: 'Register samples',
        message: $content,
        cssClass: 'copo-modal1 form-modal',
        closable: false,
        buttons: [
          {
            id: 'btnBack',
            label: '<i class="fa fa-arrow-left"></i> Back',
            cssClass: 'tiny ui basic button',
            action: function (dialogRef) {
              // Keep what was entered in case the user comes back
              writeHiddenBatchFields(
                $profileForm,
                getBatchFormValues($batchForm, options)
              );
              dialogRef.close();
            },
          },
          {
            id: 'btnSaveFirstBatch',
            label:
              '<i class="copo-components-icons glyphicon glyphicon-save"></i> Save',
            cssClass: 'tiny ui basic primary button',
            action: function (dialogRef) {
              const fields = getBatchFormValues($batchForm, options);
              const errors = checkBatchFormValues(fields, options);
              if (errors.length) {
                showBatchDialogMessage(dialogRef, errors.join('<br>'), 'error');
                return;
              }
              writeHiddenBatchFields($profileForm, fields);
              dialogRef.close();
              // Any error (e.g. Sapio failing) is shown on the profile
              // form, which stays open with what was entered
              proceed();
            },
          },
        ],
      });
    })
    .fail(function (xhr) {
      BootstrapDialog.alert(
        getResponseMessage(xhr, 'The sample types could not be loaded. Please try again.')
      );
    });
}

// The profile form's 'Save' button reads 'Next' for a new EDP profile.
// Bound to the document rather than 'body' because this script runs in the
// page's <head> before 'body' exists; the
// `event` is triggered on 'body' and goes up to the document.
$(document).on('postformload', function (event) {
  if (event.formJSON && event.dialogRef && isNewEdpProfileForm(event.formJSON, event.dialogRef)) {
    event.dialogRef
      .getButton('btnFormSave')
      .html('Next <i class="fa fa-arrow-right"></i>');
  }
});

addFormSaveInterceptor(function (formJSON, dialogRef, proceed) {
  if (!isNewEdpProfileForm(formJSON, dialogRef)) {
    return false;
  }
  openFirstBatchDialog(dialogRef, proceed);
  return true;
});

// --- An existing EDP profile: adding and changing batches ---

function buildBatchesTable(batches, options, onEdit) {
  const labels = options.labels;
  const $table = $('<table/>', { class: 'table table-condensed sample-batches-table' });
  const $headRow = $('<tr/>');
  ['#', labels.no_of_samples, labels.sample_type, labels.container_type, labels.library_type, 'Registered', '']
    .forEach((heading) => $headRow.append($('<th/>').text(heading)));
  $table.append($('<thead/>').append($headRow));

  const $body = $('<tbody/>');
  batches.forEach((batch, index) => {
    const $row = $('<tr/>');
    $row.append($('<td/>').text(index + 1));
    $row.append($('<td/>').text(batch.no_of_samples));
    $row.append($('<td/>').text(batch.sample_type));
    $row.append($('<td/>').text(batch.container_type));
    $row.append($('<td/>').text(batch.library_type || '-'));
    $row.append($('<td/>').text(batch.date_created));

    const $action = $('<td/>');
    if (batch.locked) {
      $action.append(
        $('<span/>', {
          class: 'text-muted',
          title: `${batch.submitted_sample_count} of the samples in this batch have been submitted`,
        }).html('<i class="fa fa-lock"></i> Submitted')
      );
    } else {
      $action.append(
        $('<button/>', { type: 'button', class: 'tiny ui basic button edit-sample-batch' })
          .html('<i class="fa fa-pencil"></i> Edit')
          .on('click', () => onEdit(batch))
      );
    }
    $row.append($action);
    $body.append($row);
  });
  $table.append($body);
  return $table;
}

function markProfileAsHavingBatches(profileId) {
  // The options menu then offers 'Add more samples' rather than 'Register samples'
  $(`span[id="${profileId}"]`)
    .closest('.grid')
    .find('.copo-records-panel')
    .data('has-sample-batches', true)
    .attr('data-has-sample-batches', 'true');
}

function openProfileSampleBatchesDialog(profileId, title) {
  $.when($.getJSON(SAMPLE_BATCH_OPTIONS_URL), $.getJSON(getSampleBatchesURL(profileId)))
    .done(function (optionsResponse, batchesResponse) {
      const options = optionsResponse[0];
      let batches = batchesResponse[0].batches;
      let editedBatchId = '';

      const $batchForm = buildBatchForm(options);
      const $tableHolder = $('<div/>', { class: 'sample-batches-holder' });
      const $formHeading = $('<h5/>', { class: 'sample-batch-form-heading' });
      const $content = $('<div/>')
        .append($('<div/>', { class: 'sample-batch-message' }).hide())
        .append($tableHolder)
        .append($formHeading)
        .append($batchForm);

      let dialog = null;

      const resetForm = () => {
        editedBatchId = '';
        setBatchFormValues($batchForm, null);
        $formHeading.text('New batch of samples');
        if (dialog) {
          dialog.getButton('btnSaveSampleBatch').html(
            '<i class="copo-components-icons glyphicon glyphicon-save"></i> Register samples'
          );
          dialog.getButton('btnCancelBatchEdit').hide();
        }
      };

      const editBatch = (batch) => {
        editedBatchId = batch.batch_id;
        setBatchFormValues($batchForm, batch);
        $formHeading.text('Change batch');
        dialog.getButton('btnSaveSampleBatch').html(
          '<i class="copo-components-icons glyphicon glyphicon-save"></i> Save changes'
        );
        dialog.getButton('btnCancelBatchEdit').show();
        showBatchDialogMessage(dialog, '');
      };

      const renderBatches = () => {
        $tableHolder.empty();
        if (batches.length) {
          $tableHolder
            .append($('<h5/>').text('Registered samples'))
            .append(buildBatchesTable(batches, options, editBatch));
        }
      };

      dialog = BootstrapDialog.show({
        title: title,
        message: $content,
        cssClass: 'copo-modal1 form-modal',
        size: BootstrapDialog.SIZE_WIDE,
        closable: false,
        buttons: [
          {
            label: 'Close',
            cssClass: 'tiny ui basic button',
            action: (dialogRef) => dialogRef.close(),
          },
          {
            id: 'btnCancelBatchEdit',
            label: 'Cancel change',
            cssClass: 'tiny ui basic button',
            action: () => {
              resetForm();
              showBatchDialogMessage(dialog, '');
            },
          },
          {
            id: 'btnSaveSampleBatch',
            label: '<i class="copo-components-icons glyphicon glyphicon-save"></i> Register samples',
            cssClass: 'tiny ui basic primary button',
            action: function (dialogRef) {
              const fields = getBatchFormValues($batchForm, options);
              const errors = checkBatchFormValues(fields, options);
              if (errors.length) {
                showBatchDialogMessage(dialogRef, errors.join('<br>'), 'error');
                return;
              }

              const $saveButton = dialogRef.getButton('btnSaveSampleBatch');
              $saveButton.disable();
              $saveButton.spin();
              $.ajax({
                url: getSampleBatchesURL(profileId),
                type: 'POST',
                headers: { 'X-CSRFToken': $.cookie('csrftoken') },
                data: { ...fields, batch_id: editedBatchId },
              })
                .done(function (data) {
                  batches = data.batches || batches;
                  renderBatches();
                  if (data.status === 'success') {
                    showBatchDialogMessage(
                      dialogRef,
                      editedBatchId ? 'The batch has been changed.' : 'The samples have been registered.',
                      'success'
                    );
                    markProfileAsHavingBatches(profileId);
                    resetForm();
                  } else {
                    // e.g. a locked batch or Sapio failing; nothing was saved
                    showBatchDialogMessage(dialogRef, data.message, data.status);
                  }
                })
                .fail(function (xhr) {
                  showBatchDialogMessage(
                    dialogRef,
                    getResponseMessage(xhr, 'The samples could not be registered. Please try again.'),
                    'error'
                  );
                })
                .always(function () {
                  $saveButton.enable();
                  $saveButton.stopSpin();
                });
            },
          },
        ],
      });

      renderBatches();
      resetForm();
    })
    .fail(function (xhr) {
      BootstrapDialog.alert(
        getResponseMessage(xhr, 'The registered samples could not be loaded. Please try again.')
      );
    });
}

// The 'Register samples' profile action button given to EDP profiles
// (see setup_profile_types.py). Like editing, only the profile's owner
// and data managers can register samples.
registerProfileAction('register_samples', {
  isAvailable: ({ canEditDelete }) => canEditDelete,
  getLabel: ($panel) =>
    $panel.data('has-sample-batches') ? 'Add more samples' : 'Register samples',
  run: function (profileId, $button) {
    $('.profile-ellipsis[data-toggle="popover"]').popover('hide');
    openProfileSampleBatchesDialog(profileId, $button.attr('title'));
  },
});
