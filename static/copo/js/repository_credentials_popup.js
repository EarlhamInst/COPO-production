/**
 * Shared submit-time repository-credentials popup.
 *
 * Before a submission to a repository (ENA, ...), check whether the user has
 * their own valid stored credentials. If so, submit straight away under them;
 * otherwise open a popup offering to enter their own credentials or use COPO's
 * default for this one submission. Driven entirely by the provider's fields, so
 * it works for any repository and any submission component (sample, singlecell,
 * ...) without change.
 *
 * Usage from a component's do_record_task():
 *   ensureRepositoryCredentialsThenSubmit('sample', 'ena', task, records, args_dict);
 */
(function () {
  // Read Django's CSRF token from the cookie so our POST works regardless of
  // whether a global $.ajaxSetup CSRF handler is loaded on this page.
  function getCsrfToken() {
    var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }

  function submitWithSource(component, task, records, args_dict, source) {
    args_dict['credential_source'] = source;
    form_generic_task(component, task, records, args_dict);
  }

  window.ensureRepositoryCredentialsThenSubmit = function (component, repoKey, task, records, args_dict) {
    $.getJSON('/copo/repository_credentials/status/', { repo_key: repoKey })
      .done(function (status) {
        if (status.is_valid) {
          submitWithSource(component, task, records, args_dict, 'user');
        } else {
          showRepositoryCredentialModal(status, component, task, records, args_dict);
        }
      })
      .fail(function () {
        // Status check failed: fall back to the normal flow (the server still
        // resolves user creds -> COPO default).
        form_generic_task(component, task, records, args_dict);
      });
  };

  function showRepositoryCredentialModal(status, component, task, records, args_dict) {
    var fieldsHtml = status.fields.map(function (f) {
      var type = f.secret ? 'password' : 'text';
      return (
        '<div class="form-group">' +
        '<label>' + f.label + '</label>' +
        '<input type="' + type + '" class="form-control cred-modal-input" ' +
        'data-field-name="' + f.name + '" autocomplete="off" placeholder="' +
        (f.help_text || '') + '">' +
        '</div>'
      );
    }).join('');

    var modal = $(
      '<div class="modal fade" id="repo_cred_modal" tabindex="-1" role="dialog">' +
        '<div class="modal-dialog" role="document"><div class="modal-content">' +
          '<div class="modal-header"><button type="button" class="close" data-dismiss="modal">&times;</button>' +
            '<h4 class="modal-title">' + status.label + ' credentials</h4></div>' +
          '<div class="modal-body">' +
            '<p>You have not set up your own ' + status.label +
            ' credentials. Enter them below, or submit using COPO\'s default credentials for this submission only.</p>' +
            fieldsHtml +
            '<span class="cred-modal-feedback"></span>' +
          '</div>' +
          '<div class="modal-footer">' +
            '<button type="button" class="btn btn-default cred-modal-default">Use COPO default</button>' +
            '<button type="button" class="btn btn-primary cred-modal-save">Save, validate &amp; submit</button>' +
          '</div>' +
        '</div></div>' +
      '</div>'
    );

    $('#repo_cred_modal').remove();
    $('body').append(modal);
    modal.modal('show');

    // One-off: submit under COPO's shared credentials without storing anything.
    modal.find('.cred-modal-default').on('click', function () {
      modal.modal('hide');
      submitWithSource(component, task, records, args_dict, 'copo_default');
    });

    // Save + validate the user's own credentials, then submit under them.
    modal.find('.cred-modal-save').on('click', function () {
      var feedback = modal.find('.cred-modal-feedback');
      var payload = { repo_key: status.repo_key };
      modal.find('.cred-modal-input').each(function () {
        payload[$(this).data('field-name')] = $(this).val();
      });
      feedback.removeClass('text-danger').text('Validating...');
      $.ajax({
        url: '/copo/repository_credentials/save/',
        method: 'POST',
        data: payload,
        headers: { 'X-CSRFToken': getCsrfToken() },
      })
        .done(function (resp) {
          if (resp.is_valid) {
            modal.modal('hide');
            submitWithSource(component, task, records, args_dict, 'user');
          } else {
            feedback.addClass('text-danger').text(resp.message);
          }
        })
        .fail(function (xhr) {
          feedback.addClass('text-danger').text('Error: ' + (xhr.responseText || xhr.statusText));
        });
    });
  }

  window.showRepositoryCredentialModal = showRepositoryCredentialModal;
})();
