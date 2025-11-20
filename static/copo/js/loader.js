window.addEventListener('load', () => {
  const loader = document.getElementById('loaderOverlay');

  // Hide loader displayed when page is refreshing/loading
  if (loader) loader.classList.add('hidden'); 

  // Dispatch page ready event for tour handler
  document.dispatchEvent(new Event('pageReadyForTour'));
});
