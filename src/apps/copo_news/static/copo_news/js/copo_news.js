$(document).ready(function () {
  // Set news navigation tab as active
  setNewsNavTabActive();

  // Fix misplaced read more button
  fixMisplacedReadMoreButton();

  // Set footer to bottom of page
  fixFooterToBottomOfWebPage();

  window.addEventListener('resize', () => {
    fixFooterToBottomOfWebPage();
  });

  $('body').bind('beforeunload', function () {
    if (window.location.href.includes('news')) {
      setNewsNavTabActive();
      fixMisplacedReadMoreButton();
    }
  });

  // Clear search input box on page load/refresh
  $('#newsSearchBoxID').val('');

  // Set first dropdown option as selected option when the
  // web page is loaded or refreshed if category' query
  // parameter is present in the web page's URL
  if (!window.location.href.includes('category')) {
    $('#newsCategoryFilterID').prop('selectedIndex', 0);
  } else {
    let urlParams = new URLSearchParams(window.location.search);
    let category = toTitleCase(urlParams.get('category'));
    $('#newsCategoryFilterID').val(category);
    // Show get all news items icon
    $('#getAllNewsItemsIconID').show();
  }

  // Make news-content div clickable
  $('.news-content').on('click', function () {
    let link = $(this).find('#readMoreBtnID').attr('href');
    if (link) {
      window.location.href = link;
    }
  });

  // Prevent the click event from triggering when clicking inside the a tag
  $('.news-content a').on('click', function (e) {
    e.stopPropagation();
  });

  // Navigate to the news web page
  $('#viewMoreNewsBtnID').on('click', function () {
    window.location.href = '/news';
  });

  $('#getAllNewsItemsIconID').on('click', function () {
    window.location.href = '/news';
  });

  // Navigate to the news web page when 'news-category' CSS class is clicked and filter by the clicked category on the opened new web page
  $('.news-category').on('click', function () {
    let category = $(this).text().trim().toLowerCase();

    window.location.href = `/news?category=${category}`; //filterUrl;  //'/news';
    // set $('#newsCategoryFilterID') to the clicked category
    console.log('filteredCategory: ' + category);
    $('#newsCategoryFilterID').val($(document).data('filteredCategory'));
    search_and_filter_news('No news articles available for filtered category');
  });

  // Search news items by title or content
  $('#newsSearchBoxID').on('input', function () {
    search_and_filter_news('No news articles match query');
    fixFooterToBottomOfWebPage();
  });

  // Filter news items by category
  $('#newsCategoryFilterID').on('change', function () {
    search_and_filter_news('No news articles available for filtered category');
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

function toggleMiscNewsButton(displayMessage) {
  if ($('.news-item:visible').length === 0) {
    $('#miscNewsButtonID').text(displayMessage).off('hover').show();
  } else {
    $('#miscNewsButtonID').hide();
  }
}

function search_and_filter_news(displayMessage) {
  let selectedCategory = $('#newsCategoryFilterID').val().toLowerCase();
  let searchText = $('#newsSearchBoxID').val().toLowerCase();

  $('.news-item').each(function () {
    let title = $(this).find('.news-title').text().toLowerCase();
    let content = $(this).find('.news-excerpt').text().toLowerCase();
    let category = $(this).find('.news-category').text().toLowerCase();

    if (selectedCategory === '' || category === selectedCategory) {
      if (searchText === '') {
        $(this).show();
      } else if (title.includes(searchText) || content.includes(searchText)) {
        $(this).show();
      } else {
        $(this).hide();
      }
    } else {
      $(this).hide();
    }
  });

  // Show button if no news items are displayed
  toggleMiscNewsButton(displayMessage);
}

function setNewsNavTabActive() {
  // Set current web page as active nav bar item in top navbar
  $('.nav li a').filter(function () {
    const activePage = window.location.href;
    let linkPage = `${this.href}/`;

    // The home page of the website i.e. COPO front page URL will end
    // with '//' so it needs to be omitted
    linkPage = linkPage.endsWith('//') ? this.href : linkPage;

    if (activePage === linkPage) {
      $(this).parent().addClass('active');
      $(this).parent().append('<span class="sr-only">(current)</span>');
    }
  });
}

function fixMisplacedReadMoreButton() {
  $('.news-excerpt p a').each(function () {
    let href = $(this).attr('href').trim();

    // Remove all the classes - 'centre', 'mt-10', 'mb-10' and 'mb-10"'
    // from the anchor tag

    $(this)
      .removeAttr('centre')
      .removeAttr('mt-10')
      .removeAttr('mb-10')
      .removeAttr('mb-10"');

    if (href) {
      // Remove trailing spaces from the 'href' attribute and incorrect characters
      if (href.includes('</div>')) {
        href = href.replace('</div>', '').trim();
      }

      if (href.includes('<div class=')) {
        href = href.replace('<div class=', '').trim();
      }

      $(this).attr('href', href);
    }

    // Remove the anchor tag from the paragraph tag if the 'readMoreBtnID' ID attribute is applied
    // Check if the anchor tag has the 'readMoreBtnID' attribute
    let isReadMoreBtnIDApplied = $(this).attr('id') === 'readMoreBtnID';

    if (isReadMoreBtnIDApplied) {
      //  Create 'readMoreBtnID' button div
      let $readMoreBtnDiv = $('<div/>', { class: 'centre mt-10 mb-10' });

      // Add the anchor tag to the 'readMoreBtn' div
      let $anchorTagClone = $(this).clone();
      $readMoreBtnDiv.append($anchorTagClone);

      // Find the closest '.news-excerpt' and append the 'readMoreBtn' div after it
      $(this)
        .closest('.news-content')
        .find('.news-inside')
        .find('.news-excerpt')
        .after($readMoreBtnDiv);

      // Remove the anchor tag from the paragraph tag
      $(this).detach();
    }
  });
}

function toTitleCase(str) {
  return str.replace(/(?:^|\s)\w/g, function (match) {
    return match.toUpperCase();
  });
}
//****************************** End functions block ******************************//
