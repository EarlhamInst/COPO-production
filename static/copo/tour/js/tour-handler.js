// Import Shepherd from local module.
// It was originally a .mjs file but, renamed to .js for browser MIME type compatibility.
import Shepherd from './shepherd.js';
import { globalTourMessages, globalTourOrder } from './tour-defaults.js'; // Defaults

// Expose functions globally
// (for use in other scripts due to module scoping issues)
window.startComponentTour = startComponentTour;
window.runTour = runTour;
window.queueTourStage = queueTourStage;
window.watchComponentForTour = watchComponentForTour;
window.resetAllTours = resetAllTours;

//  Global variables
const csrfToken = getCookie('csrftoken');
const tourUrl = '/copo/tour-progress/';
const localTourCache = new Set();
let activeTour = null;
let quickTourOrder = [];
let tourOverrides = {}; // Component-specific overrides
let tourStages = {}; // Component-specific tours
let tourOrder = {}; // Tour order
let tourMessages = {};

// Initialise watcher for component on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  const componentName = document.getElementById('nav_component_name')?.value;
  if (componentName) setupTourWatcher(componentName);
});

$(document).ready(async function () {
  const componentName = $('#nav_component_name').val();
  if (!componentName || !component_def) return;

  // Load tour details from component definition
  if (component_def.hasOwnProperty(componentName)) {
    const compDef = component_def[componentName];
    tourOverrides = compDef.tourMessageOverrides || {};
    tourStages = compDef.tourStages || {};
    tourOrder = compDef.tourOrder || [];
    tourMessages = {
      ...globalTourMessages, // Global/shared messages
      ...(compDef.tourMessages || {}), // Component-specific messages
    };
  }

  // Merge component-specific messages with overrides
  Object.keys(tourOverrides).forEach((key) => {
    tourMessages[key] = {
      ...tourMessages[key],
      ...tourOverrides[key],
    };
  });

  // Build tour order
  quickTourOrder = [...tourOrder, ...globalTourOrder];
  quickTourOrder = [...new Set(quickTourOrder)]; // Remove duplicates

  // Events
  $(document).on('click', '.start-tour', function () {
    showIntroPopover(startQuickTour);
  });
});

// Functions
function setupTourWatcher(componentName) {
  // Set up event listeners and state to watch for component readiness
  if (!componentName) return;
  if (window._tourWatcherStarted) return;

  let pageReady = false;
  let componentReady = false;

  const attemptTourWatch = async () => {
    if (!pageReady || !componentReady) return;
    if (window._tourWatcherStarted) return;

    window._tourWatcherStarted = true;

    await checkQueuedTour(componentName);

    if (typeof window.watchComponentForTour === 'function') {
      await window.watchComponentForTour(componentName);
    }
  };

  // Page ready event
  document.addEventListener(
    'pageReadyForTour',
    () => {
      pageReady = true;
      attemptTourWatch();
    },
    { once: true }
  );

  // Component ready
  const checkComponent = () => {
    const val = document.getElementById('nav_component_name')?.value;
    if (val === componentName) {
      componentReady = true;
      attemptTourWatch();
      return true;
    }
    return false;
  };

  // Poll every 100ms until component exists
  if (!checkComponent()) {
    const interval = setInterval(() => {
      if (checkComponent()) clearInterval(interval);
    }, 100);
  }
}

// Introduction/initial popover
function showIntroPopover(callback) {
  const introTour = new Shepherd.Tour({
    defaultStepOptions: {
      classes: 'shepherd-theme-arrows',
      scrollTo: true,
      cancelIcon: { enabled: true },
    },
  });

  introTour.addStep({
    id: 'intro',
    title: 'Page tour',
    text: `<p>This tour will guide you through the main sections on this page.</p>
    <p class="shepherd-note">Please note that your screen will be dimmed
    and regular page elements will be inaccessible during the tour mode.</p>`,
    classes: 'shepherd-step-custom',
    buttons: [
      {
        text: 'Take tour',
        classes: 'tour-start-btn',
        action: function () {
          introTour.complete();
          callback();
        },
      },
      {
        text: 'Dismiss',
        classes: 'tour-dismiss-btn',
        action: introTour.cancel,
      },
    ],
    attachTo: { element: '.start-tour', on: 'bottom' },
  });

  introTour.start();
}

// Quick tour prompt
function startQuickTour() {
  const tour = new Shepherd.Tour({
    defaultStepOptions: {
      classes: 'shepherd-theme-arrows',
      scrollTo: { behavior: 'smooth', block: 'center' },
      cancelIcon: { enabled: true },
    },
    useModalOverlay: true,
  });

  quickTourOrder.forEach((id, index) => {
    // const el = document.querySelector(`[data-tour-id~="${id}"]`);
    const $el = $(`[data-tour-id~="${id}"]:visible`);
    const el = $el.get(0);
    if (tourMessages[id] && el) {
      const isLast = index === quickTourOrder.length - 1;

      tour.addStep({
        id,
        title: tourMessages[id].title,
        text: tourMessages[id].content,
        classes: 'shepherd-step-custom',
        attachTo: {
          element: el,
          on: tourMessages[id].placement || 'bottom',
        },
        buttons: isLast
          ? [
              {
                text: 'End tour',
                classes: 'tour-end-btn',
                action: tour.complete,
              },
            ]
          : [
              {
                text: 'Next',
                classes: 'tour-next-btn',
                action: tour.next,
              },
              {
                text: 'End tour',
                classes: 'tour-end-btn',
                action: tour.complete,
              },
            ],
      });
    }
  });

  tour.start();
}

// Start tour based on a component
function startComponentTour(componentName, processName = 'overview') {
  if (activeTour) {
    console.warn('Tour already running. Skipping.');
    return activeTour;
  }

  const tour = new Shepherd.Tour({
    defaultStepOptions: {
      cancelIcon: { enabled: true },
      classes: 'shadow-lg rounded-2xl',
      scrollTo: false,
    },
    useModalOverlay: true,
  });
  activeTour = tour;

  // Get step IDs for the component and process
  const processSteps = tourStages?.[processName] || tourOrder;

  if (!processSteps || !processSteps.length) {
    console.warn(`No tour steps found for ${componentName} (${processName})`);
    return;
  }

  // Merge message overrides (component-specific)
  const mergedMessages = { ...tourMessages };
  if (tourOverrides.hasOwnProperty(componentName)) {
    const overrides = tourOverrides[componentName];
    Object.keys(overrides).forEach((key) => {
      mergedMessages[key] = { ...mergedMessages[key], ...overrides[key] };
    });
  }

  // Build tour steps
  processSteps.forEach((id, index) => {
    const message = mergedMessages[id];
    // const element = document.querySelector(`[data-tour-id~="${id}"]`);
    const $element = $(`[data-tour-id~="${id}"]:visible`);
    const element = $element.get(0);
    if (!message || !element) {
      console.warn(`No message or element found for step ${id}`);
      return;
    }

    const isLast = index === processSteps.length - 1;

    tour.addStep({
      id,
      title: message.title,
      text: message.content,
      attachTo: {
        element,
        on: message.placement || 'bottom',
      },
      buttons: isLast
        ? [{ text: 'End tour', classes: 'tour-end-btn', action: tour.complete }]
        : [
            {
              text: 'Next',
              classes: 'tour-next-btn',
              action: tour.next,
            },
            {
              text: 'End tour',
              classes: 'tour-end-btn',
              action: function () {
                tour.complete();
                activeTour = null;
              },
            },
          ],
    });
  });

  // Start the tour
  tour.start();
  return tour;
}

async function checkQueuedTour(componentName) {
  try {
    const response = await fetch(`${tourUrl}${componentName}/`);
    const data = await response.json();

    if (data.queuedStage && !data.completedStages.includes(data.queuedStage)) {
      const tour = startComponentTour(componentName, data.queuedStage);

      if (tour && tour.steps && tour.steps.length > 0) {
        // Mark the stage as completed so it wouldn't rerun
        tour.on('complete', () => markTourRun(componentName, data.queuedStage));
        tour.on('cancel', () => markTourRun(componentName, data.queuedStage));
      } else {
        console.warn(
          `No valid tour steps found for ${componentName} (${stage}). Skipping event bindings.`
        );
      }
    }
  } catch (error) {
    console.error('Error checking queued tour:', error);
  }
}

// Check and mark tour completion
async function hasTourRun(componentName, stage) {
  try {
    const res = await fetch(`${tourUrl}${componentName}/`);
    const data = await res.json();
    return data.completedStages.includes(stage);
  } catch {
    return false;
  }
}

function markTourRun(componentName, stage) {
  fetch(`${tourUrl}${componentName}/${stage}/`, {
    method: 'POST',
    headers: { 'X-CSRFToken': csrfToken },
  });
}

async function queueTourStage(component, stage, callback = null) {
  try {
    const response = await fetch(`${tourUrl}${component}/queued/${stage}/`, {
      method: 'POST',
      headers: { 'X-CSRFToken': csrfToken },
    });

    if (!response.ok)
      console.error(`Failed to queue ${stage} stage for ${component}`);

    if (typeof callback === 'function') callback();
  } catch (error) {
    console.error('Error queuing tour stage:', error);
  }
}

async function resetAllTours() {
  localStorage.clear();
  localTourCache.clear();

  try {
    await fetch(`${tourUrl}reset/`, {
      method: 'POST',
      headers: {
        'X-CSRFToken': csrfToken,
      },
    });
    console.log('All tours cleared (both local and server ones).');
  } catch (error) {
    console.warn('Error resetting tour data: ', error);
  }
}
// window.resetTours = resetAllTours;
// resetTours();

// Begin tour only if needed
async function runTour(componentName, stage) {
  const cacheKey = `${componentName}:${stage}`;
  if (localTourCache.has(cacheKey)) return;

  const alreadyRun = await hasTourRun(componentName, stage);
  if (alreadyRun) {
    console.log(
      `Tour for ${componentName}:${stage} has already been completed.`
    );
    return;
  }

  localTourCache.add(cacheKey);

  console.log(`Starting ${componentName}:${stage} tour`);
  const tour = startComponentTour(componentName, stage);

  if (tour && tour.steps && tour.steps.length > 0) {
    // Mark the stage as completed
    tour.on('complete', () => markTourRun(componentName, stage));
    tour.on('cancel', () => markTourRun(componentName, stage));
  } else {
    console.warn(
      `No valid tour steps found for ${componentName} (${stage}). Skipping event bindings.`
    );
  }
}

// Determine current stage based on counts
function getStage({ count, isTable }) {
  const hasGettingStarted =
    $('[data-tour-id~="getting_started"]').filter(function () {
      return (
        $(this).is(':visible') &&
        $(this).css('opacity') > 0 &&
        $(this).height() > 0
      );
    }).length > 0;

  let stage = null;

  if (count === 0 && hasGettingStarted) {
    stage = 'overview';
  } else if (count > 0) {
    stage = 'creation';
  }

  // Check if the tour stage exists for the for the component
  if (
    stage &&
    tourStages &&
    Object.prototype.hasOwnProperty.call(tourStages, stage)
  ) {
    return stage;
  }

  return null;
}

async function watchComponentForTour(componentName) {
  const component = get_component_meta(componentName);

  let $el = $(`#${component.tableID}`);
  if (!$el.length) {
    $el = $(`#${component.component}_data_tab_content`);
  }

  if (!$el.length) return;

  // Evaluate and run tour stage
  async function waitForInitialModals() {
    return new Promise((resolve) => {
      setTimeout(() => {
        // Check if any modal is currently open
        const modalVisible =
          $('.modal:visible').length > 0 ||
          $('.ui.modal.visible.active').length > 0 ||
          $('.modal.in').length > 0;

        if (modalVisible) {
          // If modal is open, wait until it's closed
          console.log('Tour paused...waiting for startup modal to close.');

          // Wait for either Bootstrap or Semantic UI modal to close before resuming
          const resumeTour = () => {
            console.log('Start up modal closed — resuming tour.');
            resolve();
          };

          $(document).one('hidden.bs.modal', resumeTour); // Bootstrap modal close event
          $('.ui.modal').one('hidden', resumeTour); // Semantic UI modal close event
        } else {
          // No modal open, continue the tour
          resolve();
          return;
        }
      }, 400);
    });
  }

  async function evaluateTourStage(count, isTable) {
    const stage = getStage({ count, isTable });
    if (!stage) {
      console.warn(
        `No tour stage found for "${componentName}" component with count=${count} (isTable=${isTable}). 
        Please check the tour configuration.`
      );
      return;
    }

    // Prevent running 'overview' stage when the table/div already has data
    if (stage === 'overview' && count > 0) {
      console.log(
        `Skipping 'overview' stage for ${componentName} component because count=${count}`
      );
      return;
    }

    // Check if any modal is currently open
    const modalVisible =
      $('.modal:visible').length > 0 ||
      $('.ui.modal.visible.active').length > 0 ||
      $('.modal.in').length > 0;

    if (modalVisible) {
      // If modal is open, wait until it's closed
      console.log('Tour paused...waiting for modal to close.');

      // Wait for either Bootstrap or Semantic UI modal to close before resuming
      const resumeTour = async () => {
        console.log('Modal closed — resuming tour.');
        await runTour(componentName, stage);
      };

      $(document).one('hidden.bs.modal', resumeTour); // Bootstrap modal close event
      $('.ui.modal').one('hidden', resumeTour); // Semantic UI modal close event

      return; // Stop until modal is closed
    } else {
      // No modal open, run the tour stage
      await runTour(componentName, stage);
    }
  }

  const isTable = $.fn.DataTable && $.fn.DataTable.isDataTable($el);
  if (isTable) {
    const table = $el.DataTable();

    table.on('draw', async () => {
      const rowCount = table.rows({ filter: 'applied' }).count();
      await waitForInitialModals();
      await evaluateTourStage(rowCount, true);
    });

    // Run immediately in case table already has rows
    await waitForInitialModals();
    await evaluateTourStage(table.rows({ filter: 'applied' }).count(), true);
  } else {
    // Disconnect existing observers to prevent duplicates
    const observerInstance = $el.data('observerInstance');
    if (observerInstance) {
      observerInstance.disconnect();
    }

    // Handle state on page load
    const visibleChildren = $el.children(':visible').length;
    await waitForInitialModals();
    await evaluateTourStage(visibleChildren, false);

    // Watch changes using MutationObserver
    const observer = new MutationObserver(async () => {
      clearTimeout(observer.debounce);
      observer.debounce = setTimeout(async () => {
        const visibleChildren = $el.children(':visible').length;
        await waitForInitialModals();
        await evaluateTourStage(visibleChildren, false);
      }, 500);
    });

    observer.observe($el[0], { childList: true, subtree: true });
  }

  // Manual trigger for publish stage
  // const submitButtons = document.querySelectorAll(
  //   `[data-tour-id*="submit_${componentName}"]`
  // );

  // const publishButtons = document.querySelectorAll(
  //   `[data-tour-id*="publish_${componentName}"]`
  // );

  const submitButtons = $(
    `[data-tour-id*="submit_${componentName}"]:visible`
  ).toArray();
  const publishButtons = $(
    `[data-tour-id*="publish_${componentName}"]:visible`
  ).toArray();

  submitButtons.forEach((button) => {
    button.addEventListener('click', async () => {
      // Publish stage
      if (publishButtons) {
        // Prevent rerunning the tour if it already ran
        if (!localTourCache.has(`${componentName}:publish`)) {
          if (button.dataset.tourId.includes(`publish_${componentName}`)) {
            console.log(`Triggering publish tour for ${componentName}`);
            await runTour(componentName, 'publish');
          }
        }
      }
      // Submit → release_profile stage
      if (submitButtons) {
        // Prevent rerunning the tour if it already ran
        if (!localTourCache.has(`${componentName}:release`)) {
          if (button.dataset.tourId.includes(`release_profile`)) {
            console.log(`Triggering release_profile tour for ${componentName}`);
            await runTour(componentName, 'release');
          }
        }
      }
    });
  });
}
