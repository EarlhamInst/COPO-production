//it has been updated

var dt_options = {
  scrollY: 400,
  scrollX: true,
  bSortClasses: false,
  lengthMenu: [10, 25, 50, 75, 100, 500, 1000, 2000],
  select: {
    style: 'os',
    selector: 'td:first-child',
  },
  rowId: '_id',
  paging: true,
  createdRow: function (row, data, dataIndex) {
    $(row).addClass('sample_table_row');
  },

  columnDefs: [
    {
      className: 'tickbox',
      render: function (data, type, row) {
        var filter = $('#sample_filter').find('.active').find('a').attr('href');
        if (
          filter == 'pending' ||
          filter == 'rejected' ||
          filter == 'bge_pending'
        ) {
          return "<input type='checkbox' class='form-check-input checkbox'/>";
        } else {
          return '';
        }
      },
      targets: 0,
    },
  ],
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
  processing: true,
  serverSide: true,
  // Reload DataTable on input change.
  search: {
    return: true,
  },
  ajax: {
    url: '/copo/dtol_submission/get_samples_for_profile/',
    data: function (d) {
      return {
        profile_id: $('#profile_id').val(),
        filter: $('#sample_filter').find('.active').find('a').attr('href'),
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
var sample_table;

$(document).ready(function () {
  // functions defined here are called from both copo_sample_accept_reject and copo_samples, all provide DTOL
  // functionality
  $(document).data('accepted_warning', false);
  $(document).data('isDtolSamplePage', true);

  // Disable table buttons
  // This ensures that the buttons are not enabled when a
  // profile that is clicked by default (on the left-hand
  // side of the web page) has no samples in it
  $('#accept_reject_button').find('button').prop('disabled', true);
  $('.view-images').prop('disabled', true);
  $('.download-permits').prop('disabled', true);
  $('.delete-selected').prop('disabled', true);
  $('.select-none').prop('disabled', true);
  $('.select-all').prop('disabled', true);

  // add field names here which you don't want to appear in the supervisors table
  excluded_fields = ['profile_id', 'biosample_id', '_id'];
  searchable_fields = [''];
  // populate profiles panel on left

  $(document).on('click', '.select-all', function () {
    // Note: Sample table rows within the 'Accepted Samples' tab do not have checkboxes
    // displayed to be checked therefore, they have to be programmatically clicked
    let unchecked_records = $('.form-check-input:not(:checked)');

    let unchecked = unchecked_records.length
      ? unchecked_records
      : $('#profile_samples').find('tr.sample_table_row').not('selected');

    unchecked.each(function (idx, element) {
      unchecked_records.length
        ? $(element).click()
        : $(element).children('.tickbox').click();
    });
  });

  $(document).on('click', '.select-none', function () {
    // Note: Sample table rows within the 'Accepted Samples' tab do not have checkboxes
    // displayed to be checked therefore, they have to be programmatically clicked
    let checked_records = $('.form-check-input:checked');

    let checked = checked_records.length
      ? checked_records
      : $('#profile_samples').find('tr.sample_table_row.selected');

    checked.each(function (idx, element) {
      checked_records.length
        ? $(element).click()
        : $(element).children('.tickbox').click();
    });
  });

  $(document).on('click', 'tr.sample_table_row', function (e) {
    // Note: Sample table rows within the 'Accepted Samples' tab do not have checkboxes
    // displayed to be checked therefore, they have to be programmatically clicked
    let checkbox = $($(e.target).siblings('.tickbox').find('input'));
    var cb = checkbox.length ? checkbox : $($(e.target).siblings('.tickbox'));
    cb.click();
  });

  $(document).on('click', '.delete-selected', function (e) {
    var saved_text = $('#dtol_sample_info').text();

    var checked = $('.form-check-input:checked').closest('tr');
    var sample_ids = [];
    $(checked).each(function (it) {
      sample_ids.push($(checked[it]).attr('id'));
    });
    if (sample_ids.length == 0) {
      alert('Please select samples to delete');
      return;
    }

    BootstrapDialog.show({
      title: 'Delete Samples',
      message: 'Are you sure you want to delete?',
      buttons: [
        {
          label: 'Close',
          action: function (dialogItself) {
            dialogItself.close();
          },
        },
        {
          label: 'Delete',
          action: function (dialog) {
            $('#dtol_sample_info').text('Deleting');

            var csrftoken = $.cookie('csrftoken');

            $.ajax({
              headers: { 'X-CSRFToken': csrftoken },
              url: '/copo/dtol_submission/delete_dtol_samples/',
              method: 'POST',
              data: {
                sample_ids: JSON.stringify(sample_ids),
              },
            })
              .done(function (e) {
                $('#profile_titles').find('.selected').click();
                $('#dtol_sample_info').text('Deleted');
                dialog.close();
              })
              .fail(function (e) {
                console.log(e);
              });
          },
        },
      ],
    });
  });

  $(document).on('click', '.download-permits', function (e) {
    // Note: Sample table rows within the 'Accepted Samples' tab do not have checkboxes
    // displayed to be checked therefore, they have to be programmatically clicked
    let checked = $('.form-check-input:checked').length
      ? $('.form-check-input:checked').closest('tr')
      : $('#profile_samples').find('tr.selected');

    let sample_ids = [];

    $(checked).each(function (it) {
      sample_ids.push($(checked[it]).attr('id'));
    });

    if (sample_ids.length == 0) {
      alert('Please select sample(s) to download permits for');
      return;
    }

    let csrftoken = $.cookie('csrftoken');

    // Get URLs of the permit files
    $.ajax({
      url: '/manifests/download_permits/',
      type: 'POST',
      headers: { 'X-CSRFToken': csrftoken },
      data: {
        sample_ids: JSON.stringify(sample_ids),
      },
      success: function (urls) {
        // Convert string array to array
        urls = JSON.parse(urls);

        if (!urls.length) {
          //alert user
          button_event_alert(
            'Download permits',
            'No permits exist for the selected sample record(s)'
          );
          return;
        }

        const zip = new JSZip();
        const zipFilename = 'SAMPLE_MANIFEST_PERMITS.zip';

        let count = 0;

        $.each(urls, async function (index, url) {
          const urlArr = url.split('/');
          const filename = urlArr[urlArr.length - 1];

          try {
            const file = await JSZipUtils.getBinaryContent(url);
            zip.file(filename, file, { binary: true });
            count++;

            if (count === urls.length) {
              zip.generateAsync({ type: 'blob' }).then(function (content) {
                // Trigger the download of the zip with FileSaver.js
                saveAs(content, zipFilename);
              });
            }
          } catch (err) {
            console.log(err);
          }
        });
      },
      error: function (error) {
        console.log(`Error: ${error}`);
      },
    });
    return;
  });

  $(document).on('click', '.view-images', function (e) {
    // Note: Sample table rows within the 'Accepted Samples' tab do not have checkboxes
    // displayed to be checked therefore, they have to be programmatically clicked
    let checked = $('.form-check-input:checked').length
      ? $('.form-check-input:checked').closest('tr')
      : $('#profile_samples').find('tr.selected');

    let specimen_ids = [];
    let selected_row_index;
    let specimen_id;

    $(checked).each(function (count, element) {
      selected_row_index = $(element).index();

      specimen_id = $('#profile_samples')
        .DataTable()
        .rows(selected_row_index)
        .data()[0]['SPECIMEN_ID'];

      specimen_ids.push(specimen_id);
    });

    if (specimen_ids.length == 0) {
      alert('Please select sample(s) to view images for');
      return;
    }

    let csrftoken = $.cookie('csrftoken');

    // Get URLs of the image files
    $.ajax({
      url: '/manifests/view_images/',
      type: 'POST',
      headers: { 'X-CSRFToken': csrftoken },
      data: {
        specimen_ids: JSON.stringify(specimen_ids),
      },
      success: function (urls) {
        // Convert string array to array
        urls = JSON.parse(urls);

        if (!urls.length) {
          // Alert user
          button_event_alert(
            'View images',
            'No images exist for the selected sample record(s)'
          );
          return;
        }

        let carousel_indicator_ol_tag = $('#imageModal .modal-body')
          .find('#imageCarousel')
          .find('.carousel-indicators');

        let carousel_inner_dv = $('#imageModal .modal-body')
          .find('#imageCarousel')
          .find('.carousel-inner');

        $.each(urls, async function (index, url) {
          let urlArr = url.split('/');
          let filename = urlArr[urlArr.length - 1];
          let specimen_id = filename.substring(0, filename.lastIndexOf('-'));

          if (
            $('.carousel-inner').children().length > urls.length &&
            $('.carousel-indicators').children().length > urls.length
          ) {
            $('.carousel-inner').children().empty();
            $('.carousel-indicators').children().empty();
          }

          try {
            let image_caption = `Image: ${filename}; SPECIMEN_ID: ${specimen_id}`;

            let carousel_indicator = $('<li/>', {
              'data-target': '#imageCarousel',
              'data-slide-to': index.toString(),
            });

            let carousel_inner_item = $('<div/>', {
              class: 'item',
            });

            if (index === 0) {
              carousel_indicator.addClass('active');
              carousel_inner_item.addClass('active');
            }

            // Create a clickable image that opens in a new tab
            let figcaption = $('<figcaption/>');
            figcaption.text(image_caption);

            let image = $('<img/>', {
              class: 'd-block w-100',
              src: url,
              alt: image_caption,
            });

            let image_a_tag = $('<a/>', {
              href: url,
              target: '_blank',
            }).append(image);

            let figure = $('<figure/>').append(image_a_tag);

            // Ensure that the total number of images in the carousel
            // items is equal to the number of images' urls
            if (
              $('.carousel-inner').children().length < urls.length &&
              $('.carousel-indicators').children().length < urls.length
            ) {
              figcaption.appendTo(figure);
              figure.appendTo(carousel_inner_item);

              // Append the carousel indicators to the carousel 'ol' tag
              carousel_indicator_ol_tag.append(carousel_indicator);

              // Append the carousel inner to the carousel inner 'div' tag
              carousel_inner_dv.append(carousel_inner_item);
            }

            // Show the image modal
            $('#imageModal').modal('show');
          } catch (err) {
            console.log(err);
          }
        });
      },
      error: function (error) {
        console.log(`Error: ${error}`);
      },
    });
    return;
  });

  $(document).on('click', '.form-check-input', function (el) {
    if ($('.form-check-input:checked').length) {
      $('#accept_reject_button').find('button').prop('disabled', false);
    } else {
      $('#accept_reject_button').find('button').prop('disabled', true);
    }
    $(el.currentTarget)
      .parent()
      .siblings()
      .addBack()
      .each(function (idx, el) {
        $(el).toggleClass('selected_row');
      });
  });

  $(document).on('click', '#accept_reject_button button', handle_accept_reject);

  // handle clicks on both profiles (.selectable_row), and filter (.hot_tab)
  $(document).on('click', '.selectable_row, .hot_tab', row_select);

  $(document).on('change', '#dtol_type_select', function (e) {
    $.ajax({
      url: '/copo/get_subsample_stages',
      method: 'GET',
      data: {
        stage: $(e.currentTarget).val(),
      },
      dataType: 'json',
    }).done(function (data) {
      $('#accordion').fadeOut(function () {
        $("[id^='section']").find('.collapse').collapse('hide');
        $("[id^='section']").hide();
        $(data).each(function (idx, el) {
          el = el.replace(' ', '_');
          el = 'section_' + el;
          $('#' + el).show();
        });
        $('#accordion').fadeIn(function () {
          $(document)
            .find("[id^='section']:visible:first")
            .find('.collapse')
            .collapse('show');
        });
      });
    });
  });

  $(document).on(
    'keyup',
    '#taxonid',
    delay(function (e) {
      $('#taxonid').addClass('loading-spinner');
      var taxonid = $('#taxonid').val();
      if (taxonid == '') {
        $('#species, #genus, #family, #order, #commonName').val('');
        $('#species, #genus, #family, #order, #commonName').prop(
          'disabled',
          false
        );
        return false;
      }
      $.ajax({
        url: '/copo/resolve_taxon_id',
        method: 'GET',
        data: { taxonid: taxonid },
        dataType: 'json',
      })
        .done(function (data) {
          $('#species, #genus, #family, #order, #commonName').val('');
          $('#species, #genus, #family, #order, #commonName').prop(
            'disabled',
            false
          );
          for (var el in data) {
            var element = data[el];
            $('#' + el).prop('disabled', true);
            $('#' + el).val(element);
          }
          $('.loading-spinner').removeClass('loading-spinner');
        })
        .fail(function (error) {
          BootstrapDialog.alert(error.responseText);
        });
    })
  );

  $(document).on(
    'keyup',
    '#species_search',
    delay(function (e) {
      var s = $('#species_search').val();
      $.ajax({
        url: '/copo/search_species',
        method: 'GET',
        data: { s: s },
        dataType: 'json',
      }).done(function (data) {
        var ul = $('ul', {
          class: 'species_results',
        });
        $(data).each(function (d) {
          $(ul).append('<li>', {
            html: d,
          });
        });
        $('#resultsPanel').append(ul);
      });
    })
  );

  $(document).on('click', '#species', function (e) {
    var disabled = $(e.currentTarget).attr('disabled');

    if (typeof disabled == typeof undefined && disabled !== true) {
      BootstrapDialog.show({
        title: 'Search',
        message: $('<div></div>').load(
          '/static/copo/snippets/ncbitaxon_species_search.html'
        ),
      });
    }
  });

  var profile_samples = document.getElementById('profile_samples');
  if (
    profile_samples &&
    profile_samples.getElementsByTagName('thead')[0].children.length == 0
  ) {
    $.ajax({
      url: '/copo/dtol_submission/get_sample_column_names',
      method: 'GET',
      dataType: 'json',
    })
      .fail(function (data) {
        console.log('ERROR: ' + data);
      })
      .done(function (data) {
        if (data.length) {
          var header = $('<h4/>', {
            html: 'Samples',
          });
          $('#sample_panel').find('.labelling').empty().append(header);

          var rows = [];
          rows.push({ title: '' });

          let i = 0;
          while (i < data.length) {
            if (!excluded_fields.includes(data[i])) {
              rows.push({ title: data[i], data: data[i] });
            }
            i++;
          }
          if (profile_samples) {
            if ($.fn.DataTable.isDataTable('#profile_samples')) {
              sample_table.clear().destroy();
            }
            dt_options['columns'] = rows;
            dt_options['scrollCollapse'] = true;
            dt_options['scrollX'] = true;
            dt_options['scrollY'] = 1000;
            dt_options['fixedHeader'] = true;
            sample_table = $('#profile_samples')
              .DataTable(dt_options)
              .columns.adjust()
              .draw();
            update_profile_table();
          }
        }
      });
  }
  //if (profile_samples) {
  //update_pending_samples_table()
  //}

  $('#sample_filter').bind('click', function (e) {
    let active_tab = $(e.target).attr('href');
    let download_permits_btn = $('.download-permits');
    let view_images_btn = $('.view-images');
    let delete_selected_btn = $('.delete-selected');
    let accept_reject_btn = $('#accept_reject_button');

    if (active_tab === 'processing' || active_tab === 'accepted') {
      delete_selected_btn.prop('disabled', true).hide(); // Disable 'Delete selected' button
      accept_reject_btn.hide(); // Hide 'Accept/Reject' button
    } else {
      download_permits_btn.prop('disabled', false);
      view_images_btn.prop('disabled', false);
      accept_reject_btn.show();
      delete_selected_btn.prop('disabled', false).show();
    }

    // Reset carousel on tab change
    $('#imageCarousel').carousel({ pause: true, interval: false }).carousel(0);
  });
  $(document).on('click', '#clearStatusLogBtn', function () {
    let status_content = $('.status_content');

    status_content.not(':first').remove();
    $('#sample_panel').removeClass('status_log_overlayed');

    toggle_clear_status_log_btn_interaction();

    if (status_content.hasClass('status_log_extend')) {
      status_content
        .removeClass('status_log_extend')
        .addClass('status_log_collapse');
    }
  });

  // Create a MutationObserver which detects
  // when 'dtol_sample_info' changes
  let dtol_sample_info_element = document.getElementById('dtol_sample_info');

  if ($(dtol_sample_info_element).length) {
    let observer = new MutationObserver(function (mutations, observer) {
      if (mutations[0].attributeName == 'value') {
        $(dtol_sample_info_element).change();
      }
    });

    observer.observe(dtol_sample_info_element, {
      attributes: true,
    });

    // Ensure that added content is not empty and prevent duplicates
    // from being in the status log when a new status is detected
    $(dtol_sample_info_element).change(function (e) {
      if (
        $(this).val() &&
        $('.status_content:last-child').text() != $(this).val()
      ) {
        generate_status_log($(this));
      }
    });
  }

  // Create an overlay of the status log when the
  // status log is hovered over
  $('.status_log').hover(
    function (e) {
      e.stopPropagation();

      // Only add overlay and hover properties if there is more than
      // one status in the status log
      if ($('.status_content').length != 1) {
        $(e.currentTarget)
          .removeClass('status_log_collapse')
          .addClass('status_log_extend');

        // Add an overlay over the sample panel when the status log is extended
        // i.e. move the sample panel div behind the status log
        $('#sample_panel').addClass('status_log_overlayed');
      }

      toggle_clear_status_log_btn_interaction();
    },
    function (e) {
      e.stopPropagation();
      // Only remove overlay and hover properties if there is more than
      // one status in the status log
      if ($('.status_content').length != 1) {
        $(e.currentTarget)
          .addClass('status_log_collapse')
          .removeClass('status_log_extend');

        // Remove overlay
        $('#sample_panel').removeClass('status_log_overlayed');
      }

      toggle_clear_status_log_btn_interaction();
    }
  );

  // Scroll to the bottom of the status log when the window is resized
  $(window).on('resize', () => {
    if ($('.status_log').length && $('.status_content').length != 1) {
      $('.status_log').scrollTop($('.status_log')[0].scrollHeight);
    }
  });
});

var fadeSpeed = 'fast';
var row;

function row_select(ev) {
  $('#accept_reject_button').find('button').prop('disabled', true);
  // get samples for profile clicked in the left hand panel and populate table on the right

  if ($(ev.currentTarget).is('td') || $(ev.currentTarget).is('tr')) {
    // we have clicked a profile on the left hand list
    $(document).data('selected_row', $(ev.currentTarget));
    row = $(document).data('selected_row');
    $('.selected').removeClass('selected');
    $(row).addClass('selected');
  } else {
    row = $(document).data('selected_row');
  }

  if (sample_table != undefined) {
    var filter = $('#sample_filter').find('.active').find('a').attr('href');
    if (row == undefined) {
      $('#profile_id').val('');
    } else {
      var profile_id = $(row).find('td').data('profile_id');
      $('#profile_id').val(profile_id);
    }

    $('#spinner').show();

    sample_table.ajax.reload(function () {
      if (sample_table.data().length == 0) {
        var header = $('<h4/>', {
          html: 'No Samples Found',
        });
        $('#sample_panel').find('.labelling').empty().append(header);
        $('#profile_samples_wrapper').hide();

        // Disable table buttons if profile has no samples in it
        $('.view-images').prop('disabled', true);
        $('.download-permits').prop('disabled', true);
        $('.delete-selected').prop('disabled', true);
        $('.select-none').prop('disabled', true);
        $('.select-all').prop('disabled', true);
      } else {
        var header = $('<h4/>', {
          html: 'Samples',
        });
        $('#sample_panel').find('.labelling').empty().append(header);
        $('#profile_samples_wrapper').show();
        sample_table.columns.adjust().draw();

        // Enable table buttons when profile has samples in it
        $('.view-images').prop('disabled', false);
        $('.download-permits').prop('disabled', false);
        $('.delete-selected').prop('disabled', false);
        $('.select-none').prop('disabled', false);
        $('.select-all').prop('disabled', false);
      }
    });

    $('#spinner').fadeOut('fast');
  }
  /*
    $.ajax({
        url: "/copo/update_pending_samples_table",
        data: d,
        method: "GET",
        dataType: "json"
    }).fail(function (data) {
        console.log("ERROR: " + data)
    }).done(function (data) {
            sample_table.clear();
            if (data.length) {
                var header = $("<h4/>", {
                    html: "Samples"
                })
                $("#sample_panel").find(".labelling").empty().append(header)

                sample_table.rows.add(data)

            } else {
                var content
                if (data.hasOwnProperty("locked")) {
                    content = $("<h4/>", {
                        html: "View is locked by another User. Try again later."
                    })
                } else {
                    content = $("<h4/>", {
                        html: "No Samples Found"
                    })
                }
                $("#sample_panel").find(".labelling").empty().html(
                    content
                )
                $("#accept_reject_button").find("button").prop("disabled", true)

            }
            sample_table.draw();

            $("#spinner").fadeOut("fast")

        }
    )
    */
}

function delay(fn, ms) {
  let timer = 0;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(fn.bind(this, ...args), ms || 1000);
  };
}

function update_pending_samples_table() {
  // get profiles with samples needing looked at and populate left hand column
  //check whether we are getting my profiles or all profiles
  var which_profiles = $('.profile-filter:visible')
    .find('.active')
    .find('a')
    .attr('href');
  //console.log(which_profiles)
  let columnDefs = [];

  if (which_profiles == 'my_profiles') {
    $('#accept_reject_button').show();
    $('#edit-buttons').show();
    columnDefs = [
      {
        targets: [1, 2],
        className: 'dt-center text-center',
      },
      { targets: [0], className: 'profile_title_header_my_profiles' },
    ];
  } else {
    $('#accept_reject_button').hide();
    $('#edit-buttons').hide();

    columnDefs = [
      {
        targets: [1],
        className: 'dt-center text-center',
      },
      { targets: [0], className: 'profile_title_header_all_profiles' },
    ];
  }
  $.ajax({
    url: '/copo/dtol_submission/update_pending_samples_table/',
    method: 'GET',
    dataType: 'json',
    data: { profiles: which_profiles, group: get_group_id() },
  })
    .fail(function (e) {
      console.log(e);
    })
    .done(function (data) {
      if ($.fn.DataTable.isDataTable('#profile_titles')) {
        $('#profile_titles').DataTable().clear().destroy();
      }

      $(data).each(function (d) {
        let date = new Date(data[d].date_created.$date).toLocaleDateString(
          'en-GB',
          { timeZone: 'UTC' }
        );
        let link = '<td/>';
        if (which_profiles == 'my_profiles') {
          link =
            "<td><a href='/copo/copo_sample/" +
            data[d]._id.$oid +
            "/view'><i class='fa fa-link' aria-hidden='true'></i></a></td>'";
        }
        $('#profile_titles')
          .find('tbody')
          .append(
            "<tr class='selectable_row'><td data-profile_id='" +
              data[d]._id.$oid +
              "'>" +
              data[d].title +
              '</td><td>' +
              date +
              '</td>' +
              link +
              '</tr>'
          );
      });

      $.fn.dataTable.moment('DD/MM/YYYY');
      $('#profile_titles').DataTable({
        responsive: true,
        paging: false,
        dom: '<"top"f>rt<"bottom"lp><"clear">',
        order: [[1, 'desc']],
        columnDefs: columnDefs,
        initComplete: function () {
          var api = this.api();
          if (which_profiles != 'my_profiles') {
            // Hide Office column
            api.column(2).visible(false);
          }
        },
      });

      $(document).removeData('selected_row');
      if (data.length) {
        $('#profile_titles').find('tbody').find('tr:first').click();
      } else {
        $('.hot_tab.active').click();
      }

      // Adjust the width of the table if it is 'All Profiles'
      if (which_profiles != 'my_profiles') {
        $('#profile_titles').css('width', '100%');
      }
    });
}

function handle_accept_reject(el) {
  $('#spinner').fadeIn(fadeSpeed);
  $('#accept_reject_button').find('button').prop('disabled', true);

  var checked = $('.form-check-input:checked').closest('tr');

  var button = $(el.currentTarget);
  var action;
  if (button.hasClass('positive')) {
    action = 'accept';
  } else {
    action = 'reject';
  }
  var sample_ids = [];
  $(checked).each(function (it) {
    sample_ids.push($(checked[it]).attr('id'));
  });

  $(checked).each(function (idx, row) {
    $(row).fadeOut(fadeSpeed);
    $(row).remove();
  });

  if (action == 'reject') {
    // mark sample object as rejected
    $.ajax({
      url: '/copo/dtol_submission/mark_sample_rejected',
      method: 'GET',
      data: { sample_ids: JSON.stringify(sample_ids) },
    }).done(function () {
      $('#profile_titles').find('.selected').click();
      $('#spinner').fadeOut(fadeSpeed);
    });
  } else if (action == 'accept') {
    if ($(document).data('accepted_warning')) {
      // create or update dtol submission record
      var profile_id = $('#profile_id').val();
      $('#sub_spinner').fadeIn(fadeSpeed);
      $.ajax({
        url: '/copo/dtol_submission/add_sample_to_dtol_submission/',
        method: 'GET',
        data: {
          sample_ids: JSON.stringify(sample_ids),
          profile_id: profile_id,
        },
      }).done(function () {
        $('#profile_titles').find('.selected').click();
        $('#spinner').fadeOut(fadeSpeed);
      });
    } else {
      BootstrapDialog.show({
        title: 'ENA Submission',
        message:
          'By accepting the samples, the samples will immediately be submitted to European Nucleotide Archive (ENA). This action is' +
          ' irreversible.',
        cssClass: 'copo-modal1',
        closable: true,
        animate: true,
        closeByBackdrop: false, // Prevent dialog from closing by clicking on backdrop
        closeByKeyboard: false, // Prevent dialog from closing by pressing ESC key
        type: BootstrapDialog.TYPE_INFO,
        buttons: [
          {
            label: 'Cancel',
            cssClass: 'tiny ui basic' + ' button',
            id: 'code_cancel',
            action: function (dialogRef) {
              dialogRef.close();
            },
          },
          {
            label: 'Okay',
            id: 'code_okay',
            cssClass: 'tiny ui basic button',
            action: function (dialogRef) {
              dialogRef.close();
              $(document).data('accepted_warning', true);
              // create or update dtol submission record
              var profile_id = $('#profile_id').val();
              $('#sub_spinner').fadeIn(fadeSpeed);
              $.ajax({
                url: '/copo/dtol_submission/add_sample_to_dtol_submission/',
                method: 'GET',
                data: {
                  sample_ids: JSON.stringify(sample_ids),
                  profile_id: profile_id,
                },
              }).done(function () {
                $('#profile_titles').find('.selected').click();
                $('#spinner').fadeOut(fadeSpeed);
              });
            },
          },
        ],
      });
    }
  }
}

function update_profile_table() {
  if ($('#group_id option').length == 1) {
    $('#group_id').hide();
  }
  group = get_group_id();
  if (group == 'erga') {
    $('#erga').show();

    $('#non_erga').hide();
  } else {
    $('#erga').hide();
    $('#non_erga').show();
  }
  update_pending_samples_table();
}

function generate_status_log(item) {
  let status_log = $('.status_log');

  let status_content = $('<p/>', {
    class: 'status_content',
  });

  // Append the status content to the 'status_log' div with the
  // latest status at the bottom
  if (item.hasClass('sample-alert-error')) {
    status_content.addClass('status_content_error');
  }

  status_content.html(item.val());

  // Prevent duplicates from being added to the status log
  if ($('.status_content:last-child').text() != status_content.text()) {
    status_log.append(status_content);
  }

  // Set the scrollbar to the bottom of the status log
  status_log.scrollTop($('.status_log')[0].scrollHeight);
}

function toggle_clear_status_log_btn_interaction() {
  // Disable clear status log button if there is
  // only one status in the status log
  let clear_status_log_btn = $('#clearStatusLogBtn');

  if ($('.status_content').length === 1) {
    clear_status_log_btn.prop('disabled', true).prop('title', '');
  } else {
    clear_status_log_btn
      .prop('disabled', false)
      .prop('title', 'Clear status log');
  }
}
