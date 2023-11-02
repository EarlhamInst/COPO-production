$(document).ready(function () {

    $(document).on('click', '#erga', update_pending_samples_table)
    $(document).on('click', '#non_erga', update_pending_samples_table)
    $(document).on('change', '#group_id', update_profile_table)

    


});


function get_group_id() {
    if ($('#group_id').length > 0) {
      return $('#group_id').find(':selected').val();
    } 
  }