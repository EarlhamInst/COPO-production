'use strict';
$(document).ready(function () {
  $(document).ajaxStart(function () {
    $('.saving_status').show();
  });
  $(document).ajaxStop(function () {
    $('.saving_status').hide();
  });

  $('.saving_status').hide();
  user_lookup();
  $(document).on('click', '.dropdown-menu li a', function (e) {
    $(this)
      .parents('.btn-group')
      .find('.selection')
      .text($(this).text() + '');
    $('#selected_group').html($(this).text());
    $('#selected_group').data('selected_group_id', $(this).data('group-id'));
    $('#delete_group_button').css('visibility', 'visible');
    $('#edit_group_button').css('visibility', 'visible');
    $('.in,.open').removeClass('in open');
    $('#tool_window').css('visibility', 'visible');
    get_profiles_in_group_data(e);
    get_users_in_group_data(e);
    //get_repos_in_group_data()
  });
  $(document).on('click', '#submit_group', validate_group_form);
  $(document).on('click', '#edit_group_button', show_edit_dialog);
  $(document).on('click', '#delete_group_button', show_delete_dialog);

  $('#profiles_in_group').multiSelect({
    selectableHeader:
      "<div class='custom-header'>" + $('#profile_tab_title').val() + '</div>',
    selectionHeader: "<div class='custom-header'>Added to Group</div>",
    dblClick: true,
    afterSelect: add_profile_in_group_handler,
    afterDeselect: remove_profile_in_group_handler,
  });
  $('#repos_in_group').multiSelect({
    selectableHeader: "<div class='custom-header'>Addable Repositores</div>",
    selectionHeader: "<div class='custom-header'>Added Repositories</div>",
    dblClick: true,
    afterSelect: add_repo_to_group,
    afterDeselect: remove_repo_from_group,
  });

  $(document).on('click', '.delete_cell', delete_user_row);

  //******************************Event Handlers Block*************************//
  var component = 'group';
  var copoFormsURL = '/copo/copo_forms/';
  var csrftoken = $.cookie('csrftoken');

  do_global_help(component);

  refresh_tool_tips();

  //handle task button event
  $('body').on('addbuttonevents', function (event) {
    do_record_task(event);
  });

  // Clear 'Create New Group' form when modal is closed/hidden
  $('#add_group_modal').on('hidden.bs.modal', function () {
    $('.modal-body').find('#groupName').val('');
    $('.modal-body').find('#groupDescription').val('');
    $('.helpDivRow').removeClass('errorDiv');
    $('.helpDivRowMessage').text('');
  });

  //add new component button
  $(document).on('click', '.new-component-template', function (event) {
    initiate_form_call(component);
  });

  //details button hover
  $(document).on('mouseover', '.detail-hover-message', function (event) {
    $(this).prop('title', 'Click to view ' + component + ' details');
  });

  //******************************Functions Block******************************//

  function do_record_task(event) {
    var task = event.task.toLowerCase(); //action to be performed e.g., "Edit", "Delete"
    var tableID = event.tableID; //get target table

    //retrieve target records and execute task
    var table = $('#' + tableID).DataTable();
    var records = []; //
    $.map(table.rows('.selected').data(), function (item) {
      records.push(item);
    });

    //add task
    if (task == 'add') {
      initiate_form_call(component);
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
  }

  function validate_group_form(e) {
    $('#group_form').validator('validate');
  }

  $('#group_form')
    .validator()
    .on('submit', function (e) {
      if (e.isDefaultPrevented()) {
        console.log();
      } else {
        e.preventDefault();
        // submit form to create new group
        var group_name = $('#groupName').val();
        var description = $('#groupDescription').val();
        const csrftoken = $.cookie('csrftoken');

        $.ajax({
          url: '/copo/create_group/',
          type: 'POST',
          headers: {
            'X-CSRFToken': csrftoken,
          },
          data: {
            group_name: group_name,
            description: description,
          },
          dataType: 'json',
        })
          .done(function (data) {
            let li = $('<li>').html(
              "<a href='#' data-group-id='" +
                data.id +
                "'>" +
                data.name +
                '</a>'
            );
            $('#group_dropdown_ul').append(li);
            $('#group_name_button').text(data.name + ' ');
            $('#selected_group').html(data.name);
            $('#selected_group').data('selected_group_id', data.id);
            $('#add_group_modal').modal('hide');
            $('#delete_group_button').css('visibility', 'visible');
            $('#edit_group_button').css('visibility', 'visible');
            $('#tool_window').css('visibility', 'visible');

            get_profiles_in_group_data();
            // Show success alert in the 'Info' panel
            const alertType = 'success';
            const alertMessage = 'Group created!';
            display_copo_alert(alertType, alertMessage);
          })
          .fail(function (data) {
            $('.helpDivRow').show();
            $('.helpDivRow').addClass('errorDiv');
            $('.helpDivRowMessage').text(data['responseText']);
          });
      }
    });

  function show_delete_dialog(e) {
    BootstrapDialog.show({
      title: 'Delete Group',
      message: 'Do you really want to delete this group?',
      cssClass: 'copo-modal1',
      closable: false,
      animate: true,
      type: BootstrapDialog.TYPE_WARNING,
      buttons: [
        {
          label: 'Cancel',
          cssClass: 'tiny ui basic button',
          action: function (dialogRef) {
            dialogRef.close();
          },
        },
        {
          label: "<i class='copo-components-icons fa fa-trash'></i> Delete",
          cssClass: 'tiny ui basic orange button',
          action: function (dialogRef) {
            $.ajax({
              url: '/copo/delete_group/',
              data: {
                group_id: $('#selected_group').data('selected_group_id'),
              },
              dataType: 'json',
            })
              .done(function (data) {
                var el = $(
                  "a[data-group-id='" +
                    $('#selected_group').data('selected_group_id') +
                    "']"
                );
                el.remove();
                $('#group_name_button').text('Select Group' + ' ');
                $('#selected_group').html('Select Group');
                $('#selected_group').data('selected_group_id', undefined);
                $('#delete_group_button').css('visibility', 'hidden');
                $('#edit_group_button').css('visibility', 'hidden');
                $('#tool_window').css('visibility', 'hidden');

                // Show success alert in the 'Info' panel
                const alertType = 'success';
                const alertMessage = 'Group deleted!';
                display_copo_alert(alertType, alertMessage);
              })
              .fail(function (e) {
                console.log(e);
              });
            dialogRef.close();
          },
        },
      ],
    });
  }

  function show_edit_dialog(e) {
    let group_id = $('#selected_group').data('selected_group_id');

    // Clone the modal body of the 'Add Group' modal
    let message = $('#add_group_modal .modal-body').children().clone();

    message.find('#group_form').attr('id', 'edit_group_form');
    message.find('.submitFormGroupDiv').remove();

    message.find('.groupNamelLabel').attr('for', 'editGroupName');
    message.find('#groupName').attr('id', 'editGroupName');

    message.find('groupDescriptionlLabel').attr('for', 'editGroupDescription');
    message.find('#groupDescription').attr('id', 'editGroupDescription');

    $.ajax({
      url: '/copo/view_group/',
      data: {
        group_id: group_id,
      },
      dataType: 'json',
    }).done(function (data) {
      message.find('#editGroupName').val(data['resp'].name);
      message.find('#editGroupDescription').val(data['resp'].description);
    });

    let dialog = new BootstrapDialog({
      title: 'Edit Group',
      message: message,
      closable: true,
      animate: true,
      onhidden: function () {
        $('.helpDivRow').removeClass('errorDiv');
        $('.helpDivRowMessage').text('');
      },
      buttons: [
        {
          id: 'editBtnID',
          label: 'Save',
          cssClass: 'tiny ui btn btn-primary pull-left',
          action: function (dialogRef) {
            let editedGroupName = $('#editGroupName').val();
            let editGroupDescription = $('#editGroupDescription').val();
            const csrftoken = $.cookie('csrftoken');

            $.ajax({
              url: '/copo/edit_group/',
              type: 'POST',
              headers: {
                'X-CSRFToken': csrftoken,
              },
              data: {
                group_id: group_id,
                group_name: editedGroupName,
                description: editGroupDescription,
              },
              dataType: 'json',
            })
              .done(function (data) {
                // Show success alert in the 'Info' panel
                const alertType = 'success';
                const alertMessage = 'Group updated!';
                display_copo_alert(alertType, alertMessage);
                dialogRef.close();
              })
              .fail(function (data) {
                $('.helpDivRow').show();
                $('.helpDivRow').addClass('errorDiv');
                $('.helpDivRowMessage').text(data['responseText']);
              });
          },
        },
      ],
    });

    dialog.realize();
    dialog.getModalBody().css('padding', '20px 20px 5px 20px');
    dialog.getModalBody().find();
    dialog.open();
  }

  function add_repo_to_group(values) {
    var group_id = $('#selected_group').data('selected_group_id');
    $.ajax({
      url: '/copo/add_repo_to_group/',
      method: 'GET',
      dataType: 'json',
      data: {
        repo_id: values[0],
        group_id: group_id,
      },
    })
      .done(function (d) {
        //console.log(d.resp);
      })
      .fail(function (e, d) {
        console.log(e.responseJSON.resp);
      });
  }

  function remove_repo_from_group(values) {
    //console.log(values)
    var group_id = $('#selected_group').data('selected_group_id');
    $.ajax({
      url: '/copo/remove_repo_from_group/',
      method: 'GET',
      dataType: 'json',
      data: { repo_id: values[0], group_id: group_id },
    })
      .done(function (d) {
        //console.log(d.resp);
      })
      .fail(function (e, d) {
        console.log(e.responseJSON.resp);
      });
  }

  function add_profile_in_group_handler(values) {
    var group_id = $('#selected_group').data('selected_group_id');
    $.ajax({
      url: '/copo/add_profile_to_group/',
      method: 'GET',
      dataType: 'json',
      data: {
        profile_id: values[0],
        group_id: group_id,
      },
    })
      .done(function (d) {
        console.log(d.resp);
      })
      .fail(function (e, d) {
        console.log(e.responseJSON.resp);
      });
  }

  function remove_profile_in_group_handler(values) {
    console.log(values);
    var group_id = $('#selected_group').data('selected_group_id');
    $.ajax({
      url: '/copo/remove_profile_from_group/',
      method: 'GET',
      dataType: 'json',
      data: { profile_id: values[0], group_id: group_id },
    })
      .done(function (d) {
        console.log(d.resp);
      })
      .fail(function (e, d) {
        console.log(e.responseJSON.resp);
      });
  }

  function get_users_in_group_data() {
    var group_id = $('#selected_group').data('selected_group_id');
    $.ajax({
      url: '/copo/get_users_in_group/',
      method: 'GET',
      data: { group_id: group_id },
      dataType: 'json',
    }).done(function (data) {
      $('#users_table tbody').empty();
      $(data.resp).each(function (idx, item) {
        console.log(item);
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td>' +
          item.first_name +
          ' ' +
          item.last_name +
          "</td><td class='delete_cell'>" +
          "<i class='fa fa-minus-square delete-user-button minus-color'></i>" +
          '</td>';
        $(tr).data('first_name', item.first_name);
        $(tr).data('last_name', item.last_name);
        $(tr).data('username', item.username);
        $(tr).data('email', item.email);
        $(tr).data('id', item.id);
        $(tr).appendTo('#users_table tbody');
      });
    });
  }
});

var user_lookup = function () {
  // remove all previous autocomplete divs
  $('.autocomplete').remove();
  AutoComplete(
    {
      EmptyMessage: 'No Users Found',
      Url: '/copo/get_users/',
      _Select: do_user_select,
      _Render: do_user_post,
      _Position: do_user_position,
    },
    '.user_search_field'
  );

  function do_user_select(item) {
    var tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' +
      $(item).data('first_name') +
      ' ' +
      $(item).data('last_name') +
      "</td><td class='delete_cell'>" +
      "<i class='fa fa-minus-square delete-user-button minus-color'></i>" +
      '</td>';
    $(tr).data('first_name', $(item).data('first_name'));
    $(tr).data('last_name', $(item).data('last_name'));
    $(tr).data('username', $(item).data('username'));
    $(tr).data('email', $(item).data('email'));
    $(tr).data('id', $(item).data('id'));
    $(tr).appendTo('#users_table tbody');
    add_user_to_group(tr);
  }

  function do_user_position(a, b, c) {
    // console.log(a, b, c);
  }

  function do_user_post(response) {
    if (response == '') {
      response = '[]';
    }
    response = JSON.parse(response);

    var empty,
      length = response.length,
      li = document.createElement('li'),
      ul = document.createElement('ul');

    for (var item in response) {
      try {
        li.innerHTML =
          "<div class='h5'>" +
          response[item][1] +
          ' ' +
          response[item][2] +
          "</div><span class='h6'>" +
          response[item][3] +
          '</span>';
        $(li).data('id', response[item][0]);
        $(li).data('first_name', response[item][1]);
        $(li).data('last_name', response[item][2]);
        $(li).data('email', response[item][3]);
        $(li).data('username', response[item][4]);

        //$(li).attr("data-id", doc.id);
        var styles = {
          margin: '2px',
          marginTop: '4px',
          fontSize: 'large',
        };
        $(li).css(styles);

        ul.appendChild(li);
        li = document.createElement('li');
      } catch (err) {
        console.log(err);
        li = document.createElement('li');
      }
    }
    $(this.DOMResults).empty();
    this.DOMResults.append(ul);
  }
}; //end of function

function add_user_to_group(row) {
  var user_details = get_user_details_from_row(row);
  var group_id = $('#selected_group').data('selected_group_id');
  $.ajax({
    url: '/copo/add_user_to_group/',
    data: {
      group_id: group_id,
      user_id: user_details.id,
    },
    dataType: 'json',
  });
}

function get_user_details_from_row(row) {
  var user_details = new Object();
  user_details.id = $(row).data('id');
  user_details.first_name = $(row).data('first_name');
  user_details.last_name = $(row).data('last_name');
  user_details.username = $(row).data('username');
  user_details.email = $(row).data('email');
  return user_details;
}

function delete_user_row(e) {
  var row = $(e.currentTarget).parents('tr');
  remove_user_from_group(row);
  row.remove();
}

function remove_user_from_group(row) {
  var user_details = get_user_details_from_row(row);
  var group_id = $('#selected_group').data('selected_group_id');
  // console.log(user_details);
  // console.log(group_id);
  $.ajax({
    url: '/copo/remove_user_from_group/',
    data: {
      group_id: group_id,
      user_id: user_details.id,
    },
    dataType: 'json',
  });
}

function get_repos_in_group_data() {
  var group_id = $('#selected_group').data('selected_group_id');
  $.ajax({
    url: '/copo/get_repos_for_user/',
    method: 'GET',
    data: { group_id: group_id },
    dataType: 'json',
  }).done(function (data) {
    $('#repos_in_group option').remove();
    $(data.resp).each(function (idx, el) {
      console.log(el);
      $('#repos_in_group').multiSelect('addOption', {
        value: el._id.$oid,
        text: el.url,
      });
      if (el.selected) {
        $('#repos_in_group').multiSelect('select', el._id.$oid);
      }
    });
    $('#repos_in_group').multiSelect('refresh');
  });
}

function get_profiles_in_group_data() {
  var group_id = $('#selected_group').data('selected_group_id');
  $.ajax({
    url: '/copo/get_profiles_in_group/',
    method: 'GET',
    data: { group_id: group_id },
    dataType: 'json',
  }).done(function (data) {
    $('#profiles_in_group option').remove();
    $(data.resp).each(function (idx, el) {
      $('#profiles_in_group').multiSelect('addOption', {
        value: el._id.$oid,
        text: el.title,
      });
      if (el.selected) {
        $('#profiles_in_group').multiSelect('select', el._id.$oid);
      }
    });
    $('#profiles_in_group').multiSelect('refresh');
  });
}
