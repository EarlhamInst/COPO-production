function get_profile_type() {
  return $('#profile_type').find(':selected').val();
}

function get_groups() {
  // Parse the stringified list to an array
  let stringList = $('#groups').val();
  stringList = stringList.replace(/'/g, '"');
  return JSON.parse(stringList);
}

function get_profiles_visible_length() {
  return Number($('#profiles_visible_length').val());
}

function get_profiles_total() {
  return Number($('#profiles_total').val());
}

$(document).on('document_ready', function () {
  //****************************** Event handlers block *************************//
  const component = 'profile';
  const copoProfileIndexURL = '/copo/';
  const copoAcceptRejectURL = '/copo/dtol_submission/accept_reject_sample';
  const copoVisualsURL = '/copo/copo_visualize/';
  const componentMeta = get_component_meta(component);
  const csrftoken = $.cookie('csrftoken');
  const tableID = componentMeta.tableID;
  const tableLoader = $('<div class="copo-i-loader"></div>');

  const profiles_total = get_profiles_total();
  const profiles_visible_length = get_profiles_visible_length();
  const groups = get_groups();

  $(document).data('page', 1);
  $(document).data('block_request', false);
  $(document).data('end_pagination', false);
  $(document).data('profiles_total', profiles_total);

  let grid_count = $('#grid-count');
  let grid_total = $('#grid-total');

  // Create an object to store the arguments to
  // be passed to the function
  let obj = {
    tableLoader: tableLoader,
    copoProfileIndexURL: copoProfileIndexURL,
    page: $(document).data('page'),
    tableID: tableID,
    copoVisualsURL: copoVisualsURL,
    csrftoken: csrftoken,
    component: component,
    grid_count: grid_count,
    grid_total: grid_total,
    on_scroll: false,
  };

  // Trigger refresh of the table div to reflect the changes made
  $('#copo-sidebar-info #page_alert_panel').on(
    'refreshtable2 reloadWebPage2',
    function (event) {
      event.stopPropagation();
      if (event.type === 'refreshtable2') {
        obj.page = 1;
        obj.on_scroll = false;

        // Reset the page number to 1
        $(document).data('page', obj.page);

        // Load new content or updated content
        populate_profiles_records(obj);

        // Reload web page if no profiles exist
        reload_if_no_profiles(tableID);

        // Reload the scroll handler after table refresh
        set_profile_div_scroll(obj, tableLoader);

        if (
          $(document).data('profiles_total') != Number($('#grid-count').text())
        ) {
          $(document).data('end_pagination', false);
          $(document).data('block_request', false);
        }
      } else if (event.type === 'reloadWebPage2') {
        // Trigger reload of the web page based on the value of 'grid-total'
        // i.e. when one profile record is created
        // to have the change reflected on the web page
        if ($('#grid-count').text() === '' && $('#grid-total').text() === '') {
          setTimeout(function () {
            window.location.reload();
          }, 1000);
        }
      }
    }
  );

  // Trigger reload of the web page when no profile
  // records exist to have the change reflected on
  // the web page
  $('#grid-total').on('reloadWebPage1', function () {
    reload_if_no_profiles(tableID);
  });

  // Store the title displayed when a user hovers the ellipsis/profile options icon
  $(document).data('profileOptionsTitle', $('.row-ellipsis').attr('title'));

  // Add new profile button
  $(document).on('click', '.new-component-template', function () {
    var args_dict = { profile_type: get_profile_type() };
    initiate_form_call(component, args_dict);
  });

  $(document).on('click', '#accept_reject_shortcut', function () {
    document.location = copoAcceptRejectURL;
  });

  // Display 'Accept/reject' button for sample managers
  if (groups.some((g) => g.includes('sample_managers'))) {
    $('#accept_reject_shortcut').show();
  }

  // Display empty profile message for potential first time users
  set_empty_component_message(profiles_total);

  // No profile records exist
  if (profiles_visible_length === 0) {
    $('#bottom-panel').hide();
    $('.profiles-legend').hide();
    $('.other-projects-accessions-filter-checkboxes').hide();
    return false;
  }

  initialise_popover();

  if ($('#sortProfilesBtn').length) {
    // Set first option of sort menu
    $('#sortProfilesBtn')[0].selectedIndex = 0;
  }

  grid_count.text(profiles_visible_length); // Number of profile records visible

  //  Total number of profile records for the user
  grid_total.text(profiles_total);

  let div_grid = $('div.grid');

  appendRecordComponents(div_grid);
  filter_action_menu();
  update_counts(copoVisualsURL, csrftoken, component);

  set_profile_grid_heading(div_grid, tableID); // Set profile grid heading

  profile_info_popover(div_grid); // Initialise 'view more' information popover for profile records

  $('#sortProfilesBtn').on('change', function () {
    const option_selected = this.value;
    sort_profile_records(option_selected);
  });

  $(document).data('sortByDescendingOrder', true);

  $(document).on('click', '.expanding_menu > div', function (e) {
    const el = $(e.currentTarget);
    el.closest('.grid').removeClass('grid-selected');
    el.closest('.panel-heading')
      .next('.grid-panel-body')
      .removeClass('grid-panel-body-selected');
  });

  $(document).on('click', '.item a', function (e) {
    let url;
    const el = $(e.currentTarget);
    if (el.hasClass('action')) {
      const action_type = el.data('action_type');
      let id = el.closest('.expanding_menu').attr('id');
      id = id.split('_')[1];

      if (action_type === 'release_study') {
        result = confirm('Are you sure to release the study?');
        if (result) {
          url = '/copo/copo_profile/' + id + '/release_study';
          $.ajax({
            url: url,
          })
            .done(function (data) {
              $('#study_status_' + id).html('PUBLIC');
              $('#study_release_date_' + id).html(data['study_release_date']);
              el.hide();
            })
            .fail(function (data) {
              alert(data.responseText);
            });
        }
      } else {
        url = '/copo/copo_' + action_type + '/' + id + '/view';
        document.location = url;
      }
    }
  });

  // Toggle the visibility of the button to
  // sort in ascending order or descending order
  $(document).on('click', '#sortIconID', function (e) {
    let option = $('#sortProfilesBtn').val();

    $(this).toggleClass('sort-down fa fa-sort-down');
    $(this).toggleClass('sort-up fa fa-sort-up');

    if ($('i.sort-down').length) {
      $(document).data('sortByDescendingOrder', true);
      sort_profile_records(option);
    } else {
      $(document).data('sortByDescendingOrder', false);
      sort_profile_records(option);
    }

    e.preventDefault();
  });

  // Hide the profile options popover, display 'View profile options'
  // on hover of the profile options ellipsis icon and unhighlight
  // focus on desired profile grid
  $(document).on('click', `#${tableID}`, function () {
    $('#ellipsisID[data-toggle="popover"]').popover('hide');
    $('.row-ellipsis').attr('title', $(document).data('profileOptionsTitle'));
  });

  $(document).on('click', '.copo-main', function () {
    $('#ellipsisID[data-toggle="popover"]').popover('hide');
    $('.row-ellipsis').attr('title', $(document).data('profileOptionsTitle'));
  });

  $(document).on('click', '.copo-sidebar', function () {
    $('#ellipsisID[data-toggle="popover"]').popover('hide');
    $('.row-ellipsis').attr('title', $(document).data('profileOptionsTitle'));
  });

  $(document).on('click', '#editProfileBtn', function (e) {
    let profile_id = $(e.currentTarget).closest('.ellipsisDiv').attr('id');
    let profile_type = $(e.currentTarget)
      .closest('.copo-records-panel')
      .attr('profile_type');
    if (profile_type == undefined) {
      profile_type = $(e.currentTarget)
        .closest('.copo-records-panel')
        .attr('shared_profile_type');
    }
    editProfileRecord(profile_id, profile_type);
  });

  $(document).on('click', '#deleteProfileBtn', function (e) {
    let profile_id = $(e.currentTarget).closest('.ellipsisDiv').attr('id');
    deleteProfileRecord(profile_id);
  });

  $(document).on('click', '#profileOptionsPopoverCloseBtn', function () {
    $('#ellipsisID[data-toggle="popover"]').popover('hide');
  });

  $(document).on('click', '#showMoreProfileInfoCloseBtn', function () {
    $('#showMoreProfileInfoBtn[rel="popover"]').popover('hide');
  });

  // Initial call to trigger infinite scroll once user scrolls downwards
  // to display more profile records that exist
  set_profile_div_scroll(obj, tableLoader);

  // Show a button which once a user hovers, it'll indicate that the
  // user can scroll downwards to view more profile records that were created
  let navigateToBottomOfPageBtn = $('#navigateToBottom');

  grid_count.text() < grid_total.text() && $(window).scrollTop() < 100
    ? navigateToBottomOfPageBtn.addClass('show')
    : navigateToBottomOfPageBtn.removeClass('show');

  // Navigate to the top of the web page on button clicked
  let navigateToTopOfPageBtn = $('#navigateToTop');

  $(window).on('scroll', function () {
    if ($(window).scrollTop() > 100) {
      // Hide the 'scroll down' button
      if (navigateToBottomOfPageBtn.hasClass('show'))
        navigateToBottomOfPageBtn.removeClass('show');
      // Show 'scroll up' button
      navigateToTopOfPageBtn.addClass('show');
    } else {
      navigateToTopOfPageBtn.removeClass('show');
    }
  });

  navigateToTopOfPageBtn.on('click', function (e) {
    e.preventDefault();
    $('html, body').animate(
      {
        scrollTop: 0,
      },
      '300'
    );
  });

  // On web page reload/refresh, sort profile records
  // by default sort option and method
  window.onload = () => {
    if ($('#sortProfilesBtn').length) {
      let option = $('#sortProfilesBtn').val();
      sort_profile_records(option);
    }
  };

  // Programmatically, scroll down the web page a little if a user decides to
  // click the button which indicates on hover to scroll down to view more profiles
  navigateToBottomOfPageBtn.click(function () {
    $('html, body').animate(
      {
        scrollTop: document.body.scrollHeight + 30,
      },
      'slow'
    );
    navigateToBottomOfPageBtn.removeClass('show'); // Hide 'scroll down' button
  });
}); // End document ready

//****************************** Functions block ******************************//

function initialise_popover() {
  // Profile records exist
  // Initialise the popover 'View profile options' for each profile record

  let popover = $('#ellipsisID[data-toggle="popover"]')
    .popover({
      sanitize: false,
    })
    .click(function (e) {
      $(this).popover('toggle');
      $('#ellipsisID[data-toggle="popover"]').not(this).popover('hide');
      e.stopPropagation();
    })
    .on('show.bs.popover', function (e) {
      $('.row-ellipsis').attr('title', ''); // Hide 'View profile options' title from appearing in the popover on hover

      // Set content of the popover
      const $content = $('<div></div>');
      const $editButton = $(
        '<button id="editProfileBtn" class="btn btn-sm btn-success" title="Edit record"><i class="fa fa-pencil"></i>&nbsp;Edit</button>'
      );
      const $deleteButton = $(
        '<button id="deleteProfileBtn" class="btn btn-sm btn-danger" title="Delete record"><i class="fa fa-trash-can"></i>&nbsp;Delete</button>'
      );

      $deleteButton.css('margin-left', '15px');
      $content.append($editButton);
      $content.append($deleteButton);

      component_def['profile']['recordActions'].forEach((item) => {
        var action = record_action_button_def[item];
        const $button = $(
          '<button id="' +
            item +
            '" class="btn btn-sm btn-primary" title="' +
            action['title'] +
            '"><i class="' +
            action['icon_class'] +
            ' "></i>&nbsp;' +
            action['label'] +
            '</button>'
        );
        $button.css('margin-top', '10px');
        $content.append($button);
        $content.append($button);
      });

      // Apply the content to the popover
      popover.attr('data-content', $content.html());
    });
}

function populate_profiles_records(obj) {
  // Save current scroll position before refreshing the table
  const scroll_position = $(window).scrollTop();

  // Destructure properties from obj
  let {
    tableLoader,
    copoProfileIndexURL,
    page,
    tableID,
    copoVisualsURL,
    csrftoken,
    component,
    grid_count,
    grid_total,
    on_scroll,
  } = obj;

  $.ajax({
    type: 'GET',
    url: copoProfileIndexURL,
    data: {
      page: page,
    },
    success: function (data) {
      if (data.end_pagination) {
        if (
          $(document).data('profiles_total') === Number($('#grid-count').text())
        ) {
          $(document).data('end_pagination', true);
          $(document).data('block_request', true);
        } else {
          $(document).data('end_pagination', false);
          $(document).data('block_request', false);
        }
      } else {
        $(document).data('block_request', false);
      }

      // Empty the table if not scrolling
      if (!on_scroll) {
        $(`#${tableID}`).empty();
      }

      let content = $(data.content);

      // Appends the html template from the 'copo_profile_record.html' to the 'copo_profiles_table' div
      // Check if the content already exists in the div, if it does, replace it with the new content
      let existing_records = $(`#${tableID}`).find('.grid');

      // Iterate through incoming records
      content.each(function () {
        let incoming_record_id = $(this).find('.row-title span').attr('id');

        let existing_record = existing_records
          .find(`#${incoming_record_id}`)
          .closest('.grid');

        if (existing_record.length) {
          // Replace the existing record with the incoming record
          existing_record.replaceWith($(this));
        } else {
          // Append the new record if it doesn't exist
          // only if the welcome message is not visible
          // since the web page will reload when the
          // first record is created
          let welcome_message = $('.page-welcome-message');

          if (!welcome_message.is(':visible')) {
            $(`#${tableID}`).append($(this));
          }
        }
      });

      let div_grid = $('div.grid');
      appendRecordComponents(div_grid); // Adds 'Components' buttons

      // Initialise functions for the profile grids beyond the 8 records that are shown by default
      refresh_tool_tips(); // Refreshes/reloads/reinitialises all popover and dropdown functions
      initialise_loaded_records(copoVisualsURL, csrftoken, component);

      set_profile_grid_heading(div_grid, tableID); // Set profile grid heading
      profile_info_popover(div_grid); // Initialise 'view more' information popover for profile records

      grid_count.text(div_grid.length); // Increment the number of profile records displayed

      // Set the total number of profile records
      grid_total.text(data.profiles_total);
      $(document).data('profiles_total', data.profiles_total);

      tableLoader.remove(); // Remove loading spinner

      // Restore scroll position after the content is loaded
      $(window).scrollTop(scroll_position);
    },
    error: function () {
      alert(`Couldn't retrieve ${component}s!`);
    },
  });
}

function appendRecordComponents(grids) {
  // loop through each grid
  grids.each(function () {
    let record_id = $(this).closest('.grid').find('.row-title span').attr('id');

    let copoRecordsPanel = $(this).closest('.grid').find('.copo-records-panel');
    let profile_type =
      copoRecordsPanel.attr('profile_type') ||
      copoRecordsPanel.attr('shared_profile_type');

    if (profile_type) {
      profile_type = profile_type.toLowerCase().trim();
    }
    // Add component buttons to the menu for each profile record
    let menu = $(this).closest('.grid').find('#expanding_menu');
    let component_buttons;
    $(menu).attr('id', 'menu_' + record_id);
    component_buttons = append_component_buttons(record_id, profile_type);
    $(menu).find('.comp').append(component_buttons);
  });
}

function editProfileRecord(profileRecordID, profileType) {
  const component = 'profile';
  let csrftoken = $.cookie('csrftoken');

  $('#ellipsisID[data-toggle="popover"]').popover('hide'); // Hides the popover

  $.ajax({
    url: copoFormsURL,
    type: 'POST',
    headers: { 'X-CSRFToken': csrftoken },
    data: {
      task: 'form',
      component: component,
      target_id: profileRecordID,
      profile_type: profileType,
    },
    success: function (data) {
      json2HtmlForm(data);
    },
    error: function () {
      alert(`Couldn't build ${component} form!`);
    },
  });
}

function deleteProfileRecord(profileRecordID) {
  const component = 'profile';
  const copoDeleteProfile = '/copo/copo_profile/delete';
  let csrftoken = $.cookie('csrftoken');

  $('#ellipsisID[data-toggle="popover"]').popover('hide'); // Hides the popover

  // Show a modal dialog to confirm if a user would like to delete the profile
  BootstrapDialog.show({
    title: 'Delete Profile',
    message:
      '<b>Are you sure that you would like this profile to be deleted?</b>' +
      '</br></br>You<strong> would not</strong> be able to retrieve the profile after it has been deleted.',
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
        label: 'Confirm',
        cssClass: 'tiny ui basic button dialog_confirm',
        action: function (dialogRef) {
          $.ajax({
            url: copoDeleteProfile,
            type: 'POST',
            headers: { 'X-CSRFToken': csrftoken },
            data: {
              task: 'validate_and_delete',
              component: component,
              target_id: profileRecordID,
            },
          })
            .done(function () {
              // Show a feedback message to the user
              const action_feedback = {
                message: 'Profile deleted!',
                status: 'success',
              };

              do_crud_action_feedback(action_feedback);

              // Remove the deleted profile record from the web page
              document
                .getElementById(profileRecordID)
                .closest('.grid')
                .remove();

              // Close any other dialog that might be opened still
              $.each(BootstrapDialog.dialogs, function (id, dialog) {
                dialog.close();
              });

              // Add an event listener that will reload a particular div to
              // reflect the changes made after a record is deleted
              // This ensures that the 'grid_count' is updated
              var event = jQuery.Event('refreshtable2');
              $('#copo-sidebar-info #page_alert_panel').trigger(event);

              // Decrement the total number of profile records displayed
              $('#grid-total').text(profiles_total - 1);

              // Refresh the web page when no profile records exist
              if ($('#grid-total').text() === '0') {
                // Hide divs
                $('#sortProfilesDiv').hide();
                $('#bottom-panel').hide();
                $('.profiles-legend').hide();
                $('.other-projects-accessions-filter-checkboxes').hide();

                var event = jQuery.Event('reloadWebPage1');
                $('#grid-total').trigger(event);
              }
            })
            .fail(function (data_response) {
              email = $('#copo_email').val();
              const message =
                "Profile couldn't be removed. Only profiles that have no datafiles or" +
                ' samples associated can be deleted.';
              let $content = '<div>';

              $content +=
                '<div style="margin-bottom: 10px; padding-bottom: 15px; font-weight: bold">' +
                message +
                '</div>';
              $content += '<p style="margin-top:10px">Please contact ';
              $content +=
                '<a style="text-decoration: underline;" href="mailto:' +
                email +
                '">' +
                email +
                '</a> ';
              $content += 'if you would like this profile to be deleted.</p>';
              $content += '</div>';

              BootstrapDialog.show({
                title: 'Profile deletion - error',
                message: $content,
                cssClass: 'copo-modal1',
                closable: true,
                animate: true,
                type: BootstrapDialog.TYPE_DANGER,
              });
            });
        },
      },
    ],
  });
}

function sort_profile_records(option) {
  // Determine the query selector
  let selector = (element) =>
    new Date(
      element.querySelector('.date_createdDiv').getAttribute('date_created')
    ).getTime(); // 'date_created' selector
  let isValueNumeric = false;

  switch (option) {
    case 'date_created':
      selector = (element) =>
        new Date(
          element.querySelector('.date_createdDiv').getAttribute('date_created')
        ).getTime();
      isValueNumeric = true;
      break;
    case 'title':
      // Remove parentheses if present from title
      selector = (element) =>
        element
          .querySelector('.row-title span')
          .innerText.replace(/\s*\(.*?\)\s*/g, '');
      break;
    case 'type':
      selector = (element) =>
        element
          .querySelector('.copo-records-panel')
          .getAttribute('profile_type');
      break;
    default:
      isValueNumeric = true;
      selector = (element) =>
        new Date(
          element.querySelector('.date_createdDiv').getAttribute('date_created')
        ).getTime(); // 'date_created' selector
  }

  // Choose the order method
  const descendingOrder = $(document).data('sortByDescendingOrder');
  const isNumeric = isValueNumeric;

  // Select all (profile grid) elements
  const elements = [...document.querySelectorAll('.grid')];

  // Find parent node
  const parentElement = elements[0].parentNode;

  // Sort the elements
  const collator = new Intl.Collator(undefined, {
    numeric: isNumeric,
    sensitivity: 'base',
  });

  elements
    .sort((elementA, elementB) => {
      const [firstElement, secondElement] = descendingOrder
        ? [elementB, elementA]
        : [elementA, elementB];
      const textOfFirstElement = selector(firstElement);
      const textOfSecondElement = selector(secondElement);
      return collator.compare(textOfFirstElement, textOfSecondElement);
    })
    .forEach((element) => parentElement.appendChild(element));
}

function do_render_profile_counts(data) {
  if (data.profiles_counts) {
    const stats = data.profiles_counts;

    for (let i = 0; i < stats.length; ++i) {
      const stats_id = stats[i].profile_id + '_';
      if (stats[i].counts) {
        for (let k in stats[i].counts) {
          if (stats[i].counts.hasOwnProperty(k)) {
            const count_id = stats_id + k;
            $('#' + count_id).html(stats[i].counts[k]);
          }
        }
      }
    }
  }
}

function get_title_by_value(value) {
  // Function to get the title from the profile type
  // dropdown menu based on a given value
  return $("#profile_type option[value='" + value + "']").attr('title');
}

function display_profiles_legend(legend_data) {
  $.each(legend_data, function (index, element) {
    let acronym = element.profileTypeAcronym.includes('Shared')
      ? 'Shared'
      : element.profileTypeAcronym;

    let type = acronym.includes('Shared')
      ? element.profileTypeAcronym
      : get_title_by_value(element.profileType);

    let colour = element.profileTypeColour;

    // Create profile type legend item
    let $legendItem = '<li class="profiles-legend-group-item">';
    $legendItem +=
      '<i class= "fa fa-info-circle profiles-legend-info-icon" title= "' +
      type +
      '"> </i>';
    $legendItem +=
      '<span class="fa fa-circle profiles-legend-circle" style="color:' +
      colour +
      '"></span>';
    $legendItem += acronym.toUpperCase();
    $legendItem += '</li>';

    $('.profiles-legend').find('.profiles-legend-group').append($legendItem);
  });
}

function set_copo_sidebar_info_padding() {
  if ($('#page_alert_panel').text().trim() === '') {
    $('.copo-sidebar-tabs')
      .find('#profilesLegendDivID')
      .css('padding-top', '100px');
  } else {
    $('#profilesLegendDivID').css('padding-top', '0');
  }
}

function update_counts(copoVisualsURL, csrftoken, component) {
  $.ajax({
    url: copoVisualsURL,
    type: 'POST',
    headers: {
      'X-CSRFToken': csrftoken,
    },
    data: {
      task: 'profiles_counts',
      component: component,
    },
    success: function (data) {
      do_render_profile_counts(data);
    },
    error: function () {
      alert(`Couldn't retrieve ${component}s information!`);
    },
  });
}

function append_component_buttons(record_id, profile_type) {
  //components row
  const components = get_profile_components(profile_type);
  const componentsDIV = $('<div/>', {
    class: 'item',
  });

  components.forEach(function (item) {
    // skip 'profile' entry metadata
    // skip 'accessions_dashboard' entry metadata
    if (
      item.component === 'profile' ||
      item.component === 'accessions_dashboard'
    ) {
      return false;
    }

    let component_link = '#';
    if (item.url != undefined)
      component_link = item.url.replace('999', record_id);

    // Create button html
    let pcomponent_name_div = $('<div></div>')
      .attr('class', `tiny ui button pcomponent-color ${item.color}`)
      .append('<i class="pcomponent-icon ' + item.iconClass + '"></i>')
      .append(
        '<span class="pcomponent-name" style="padding-left: 3px;">' +
          item.title +
          '</span>'
      );

    // Add border radius to all components in the 'Components' menu
    // i.e. Do not show the count for all components in the 'Components' menu
    pcomponent_name_div.css('border-radius', '.28571429rem');

    let buttonHTML = $('<a></a>')
      .attr('title', 'Navigate to ' + item.title)
      .attr('href', component_link)
      .attr('class', 'tiny ui labeled button pcomponent-button')
      .attr('tabindex', '0')
      .css('margin', '3px 15px 3px 3px') // Set the component buttons to the same width
      .append(pcomponent_name_div);

    componentsDIV.append(buttonHTML);
  });

  return componentsDIV;
}

function filter_action_menu() {
  $('.copo-records-panel').each(function (idx, el) {
    let t = $(el).attr('profile_type');
    let shared_t = $(el).attr('shared_profile_type');
    let s = $(el).attr('study_status');
    study_status = '';

    if (s != undefined) {
      study_status = s.toUpperCase();
    }

    if (shared_t && !t) {
      t = shared_t;
    }

    $(el).find('a[profile_component]').hide();
    $(el)
      .find('a[profile_component=' + t + ']')
      .show();

    if (s == undefined || s != 'PRIVATE') {
      $(el).find("a[data-action_type ='release_study']").hide();
    }
  });
}

function set_profile_grid_heading(grids, tableID) {
  let profiles_legend_lst = [];
  let existing_records = $(`#${tableID}`).find('.grid');

  grids.each(function () {
    $(this)
      .find('.copo-records-panel')
      .each(function (idx, el) {
        const profile_type = $(el).attr('profile_type');

        let record_id = $(this).find('.row-title span').attr('id');
        let existing_record = existing_records
          .find(`#${record_id}`)
          .closest('.grid');

        let colour;
        let acronym;

        if (profile_type) {
          acronym = profile_type.toUpperCase();
          colour =
            profile_type_def[profile_type.toLowerCase()]['widget_colour'];

          if ($(el).attr('shared_profile_type') === '') {
            // Remove 'shared_profile_type' attribute
            $(el).removeAttr('shared_profile_type');
          }
        } else {
          acronym = 'Shared With Me';
          colour = '#f26202';

          // Remove 'profile_type' attribute if it exists
          if ($(el).attr('profile_type') === '') {
            $(el).removeAttr('profile_type');
          }
        }

        if (existing_record.length) {
          // If the record already exists, update the heading and color
          existing_record
            .find('.panel-heading')
            .css('background-color', colour);

          // Remove existing acronym if it exists
          existing_record
            .find('.panel-heading')
            .find('.row-title span small')
            .remove();

          // Add new acronym
          existing_record
            .find('.panel-heading')
            .find('.row-title span')
            .append('<small> (' + acronym.toUpperCase() + ') </small>');
        } else {
          // If the record is new, set the heading and color
          if ($(el).attr('shared_profile_type') === '')
            $(el).removeAttr('shared_profile_type'); // Remove 'shared_profile_type' attribute if empty

          $(el)
            .find('.panel-heading')
            .find('.row-title span')
            .append('<small> (' + acronym.toUpperCase() + ') </small>');
          $(el).find('.panel-heading').css('background-color', colour);
        }

        // Add profile type legend data if it is not already in the list/displayed
        let legend_data = {
          profileType: profile_type,
          profileTypeAcronym: acronym,
          profileTypeColour: colour,
        };

        let current_profile_legendData = $(
          '.profiles-legend-group-item'
        ).text();

        if (
          !profiles_legend_lst
            .map((x) => x.profileType)
            .includes(profile_type) &&
          !current_profile_legendData.includes(acronym)
        ) {
          profiles_legend_lst.push(legend_data);
        }
      });
  });

  display_profiles_legend(profiles_legend_lst);
  set_copo_sidebar_info_padding();
}

function initialise_loaded_records(copoVisualsURL, csrftoken, component) {
  filter_action_menu();
  update_counts(copoVisualsURL, csrftoken, component);

  $('.expanding_menu > div').click(function (e) {
    const el = $(e.currentTarget);
    el.closest('.grid').removeClass('grid-selected');
    el.closest('.panel-heading')
      .next('.grid-panel-body')
      .removeClass('grid-panel-body-selected');
  });

  $('#profileOptionsPopoverCloseBtn').click(function () {
    $('#ellipsisID[data-toggle="popover"]').popover('hide');
  });

  $('#showMoreProfileInfoCloseBtn').click(function () {
    $('#showMoreProfileInfoBtn[rel="popover"]').popover('hide');
  });

  // Initialise the popover 'View profile options' for each profile record
  initialise_popover();
}

function profile_info_popover(grids) {
  grids.each(function () {
    let showMoreProfileInfoBtn = $(this)
      .closest('.grid')
      .find('#showMoreProfileInfoBtn[rel="popover"]');

    showMoreProfileInfoBtn
      .popover({
        html: true,
        trigger: 'click',
        sanitize: false,
        title: function () {
          let $showMoreProfileInfoCloseBtn =
            '<i id="showMoreProfileInfoCloseBtn" class="fa fa-times pull-right"></i>';
          return $showMoreProfileInfoCloseBtn;
        },
        content: function (e) {
          return $(this)
            .closest('.grid-panel-body')
            .find('#showMoreProfileInfoContent')
            .children('.popover-content')
            .html();
        },
      })
      .click(function (e) {
        e.preventDefault(); // Prevents one from being automatically redirected to the top of the page
        $(this).popover('toggle');
        $('#showMoreProfileInfoBtn[rel="popover"]').not(this).popover('hide');
        e.stopPropagation();

        // Initialise the tooltip for the associated type info icon
        // if the profile has associated types to display
        if (
          $(this).popover().is(':visible') &&
          $(this).popover().find('.associated_type_info_icon')
        ) {
          $('.associated_type_info_icon').tooltip();
        }

        // Initialise the tooltip for the sequencing centre info icon
        // if the profile has sequencing centres to display
        if (
          $(this).popover().is(':visible') &&
          $(this).popover().find('.sequencing_centre_info_icon')
        ) {
          $('.sequencing_centre_info_icon').tooltip();
        }
      });
  });
}

function reload_if_no_profiles(tableID) {
  if (
    $('#grid-total').text() === '0' ||
    $(`#${tableID}`).children().length === 0
  ) {
    setTimeout(function () {
      window.location.reload();
    }, 1000);
  }
}

function set_profile_div_scroll(obj, tableLoader) {
  $(window)
    .off('scroll')
    .on('scroll', function () {
      const margin = $(document).height() - $(window).height() - 200;
      const profiles_total = $(document).data('profiles_total');

      let block_request = $(document).data('block_request');
      let end_pagination = $(document).data('end_pagination');
      let page = $(document).data('page');
      let grid_count = Number($('#grid-count').text());

      // Calculate max number of pages
      const profiles_per_page = 8;
      const max_pages = Math.ceil(profiles_total / profiles_per_page);

      // Trigger infinite scroll once user scrolls downwards to
      // display more profile records that exist
      if (profiles_total > grid_count) {
        block_request = false;
      }

      if (profiles_total === grid_count) {
        end_pagination = true;
      } else {
        end_pagination = false;
      }

      // Update the values of the variables
      $(document).data('block_request', block_request);
      $(document).data('end_pagination', end_pagination);

      if ($(window).scrollTop() > margin && !block_request && !end_pagination) {
        // Increment the page and load new or updated content
        if (page < max_pages) {
          page += 1; // Increment the page if more pages are available
          $(document).data('page', page);
          $('#component_table_loader').append(tableLoader); // Show loading spinner

          obj.page = page;
          obj.on_scroll = true;
          populate_profiles_records(obj);
        } else {
          // Prevent further scrolling when all pages are loaded
          $(document).data('end_pagination', true);
          $(document).data('block_request', true);
        }
      }
    });
}
