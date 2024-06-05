$(document).ready(function () {
  // Set footer to bottom of page
  fixFooterToBottomOfWebPage();

  window.addEventListener('resize', () => {
    fixFooterToBottomOfWebPage();
  });
}); // End document ready

//****************************** Functions block ******************************//

function fixFooterToBottomOfWebPage() {
  let height_diff = $(window).height() - $('body').height();

  if (height_diff > 0) {
    $('#footer').css('margin-top', height_diff);
  }
}
