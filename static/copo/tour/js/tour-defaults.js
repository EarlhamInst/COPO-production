// Global tour order
export const globalTourOrder = [
  'notifications_icon',
  'home_icon',
  'analytics_icon',
  'news_icon',
  'help_icon',
  'about_icon',
  'settings_icon',
];

// Global tour messages
export const globalTourMessages = {
  about_icon: {
    title: 'About',
    content:
      'Click here to view information about the COPO platform, including its privacy policy and terms of use.',
  },
  accept_reject_samples_title_button: {
    title: 'Review samples',
    content: `Click this button to access the <strong>Accept/Reject</strong>  
    web page to review samples uploaded for your approval.<br><br>
    Evaluate the metadata and associated data for each sample before accepting or rejecting them.`,
  },
  add_record_button: {
    title: 'Add record',
    content: `Click this button to add a new record.<br><br>
    This button performs the same action as  
    <button class="circular tiny ui icon primary button no-click"><i class="icon add sign"></i>
    </button> located at the top right of the table. It is provided here for convenience.`,
  },
  analytics_icon: {
    title: 'Analytics',
    content:
      'Access analytics and visualisations about metadata submissions and usage in COPO such as: <ul><li>Statistics</li><li>Tree of Life Dashboard</li><li>Accessions Dashboard</li><li>API Endpoints</li></ul>',
  },
  clear_selection_button: {
    title: 'Clear selection',
    content: `Click this button to clear all selected rows in the table.<br><br>
    This is useful for deselecting items.`,
  },
  component_legend: {
    title: 'Data submission status legend',
    content: `This legend explains the meaning of different colours that highlight the rows in the table.<br><br>
    Hover over each <i class="fa fa-info-circle"></i> for detailed information.<br><br>
    <div class="shepherd-note">To track the status of your data submissions, refer to the following columns in the table:
    <ul><li><strong>STATUS</strong></li><li><strong>ENA FILE UPLOAD STATUS</strong></li>
    <li><strong>ENA FILE PROCESSING STATUS</strong></li></ul></div>`,
    placement: 'left',
  },
  component_options: {
    title: 'Checklist options',
    content: `Use this dropdown to select the checklist that you would like to work with.<br><br>
    <p class="shepherd-note">A checklist defines the metadata fields required for your data
    submission based on the type of data you are submitting.<br><br>
    Metadata is information that describes your data such as collection
    date, instrument model or specimen ID.</p>`,
  },
  component_table: {
    title: 'Uploaded data',
    content: `View and manage the data that you have uploaded in this table.<br><br>
      To submit it, select one or more records in this table then, click 
      <button class="tiny ui basic teal button submit-btn no-click">
      <i class="fa fa-info-circle"></i>&nbsp;Submit</button> located 
      at the top right of the table.<br><br>`,

    placement: 'right',
  },
  delete_record_button: {
    title: 'Delete',
    content: `Click this button to delete uploaded items.<br><br>
    Select <strong>at least one record</strong> in the table first before performing this action.<br><br>
    Deleting records action cannot be undone so please proceed with caution.`,
  },
  download_blank_manifest_title_button: {
    title: 'Download blank manifest',
    content: `Click this button to download a manifest template.<br><br>
    A manifest is a spreadsheet that helps you organise and record metadata.<br><br>
    Metadata is information that describes your data such as collection date, instrument model or specimen ID.<br><br>
    <p class="shepherd-note">The terms <i>manifest</i> and <i>spreadsheet</i> are often used interchangeably.</p>`,
  },
  download_sop_title_button: {
    title: 'Download standard operating procedure',
    content: `Use this button to download the Standard Operating Procedure (SOP) document for samples for your project.<br><br>
    The SOP provides detailed instructions for completing the sample manifest, including explanations of each field,
    how to fill them, and acceptable values for the sample metadata.`,
  },
  download_permits_button: {
    title: 'Download permits',
    content: `Click this button to download permit files.<br><br>
    Select <strong>at least one record</strong> in the table first. The download will include permits linked
    to the selected record.<br><br>
    Selecting multiple records will produce a zip file with the permits if the
    selected records have different permits.<br><br>
    <p class="shepherd-note">Permits are documents that provide compliance that samples can be collected, 
    transferred, stored and used in accordance with legal and institutional requirements.</p>`,
  },
  download_manifest_record_button: {
    title: 'Download manifest',
    content: `Use this button to download a spreadsheet with the data you previously uploaded.<br><br>
    Select <strong>one record</strong> in the table first. The download will include all the data.`,
  },
  edit_record_button: {
    title: 'Edit record',
    content: `Click this button to edit a record.<br><br>
    Select <strong>a record</strong> in the table first before performing this action.`,
  },
  export_csv_button: {
    title: 'Export data to CSV',
    content: `Use this button to export the uploaded data to a CSV file, including accession numbers 
      (if applicable) and all visible information.`,
  },
  getting_started: {
    title: 'Welcome to this page',
    content: `Here's a quick overview to help you get oriented.<br><br>
      Click <b>Next</b> to continue the tour and walk through the main actions.`,
    placement: 'right',
  },
  help_icon: {
    title: 'Help',
    content:
      'Access useful resources such as: <ul><li>Manifest templates</li><li>Documentation</li><li>Contact form</li></ul>',
  },
  home_icon: {
    title: 'Home',
    content: 'Return to the Home page using this icon.',
  },
  new_component_title_button: {
    title: 'Create a new entry',
    content:
      'Use this button to open a form or upload a spreadsheet where you can add a new entry relevant to the component you are on.',
  },
  new_spreadsheet_title_button: {
    title: 'Add (or update) data',
    content: `
        Use this button to upload a spreadsheet to <b>add new data</b> or <b>update existing ones</b>.<br><br>
        The system automatically detects and processes new versus existing data.<br><br>
        <p class="shepherd-note">New data must be on a separate spreadsheet. The terms <i>manifest</i> and 
        <i>spreadsheet</i> are often used interchangeably.</p>`,
  },
  news_icon: {
    title: 'News',
    content: 'View the latest and past developments about the COPO platform.',
  },
  notifications_icon: {
    title: 'Notifications',
    content: 'Click this icon to view system notifications.',
  },

  profile_component_icon_navigation_pane: {
    title: 'Navigation pane',
    content: `Use this pane to navigate between different pages (or components) associated with the selected profile. 
      Hover over each icon to view its name.<br><br>
      Alternatively, return to the desired profile on the <b>Work Profiles</b> page to view the available
      components in the <strong>Components</strong> column.<br><br>
      <p class="shepherd-note">Components are research objects or pages linked to a profile,
      each providing access to specific functionalities.</p> `,
  },
  profile_title: {
    title: 'Profile title',
    content: `This is the name of the work profile that you are working with.<br><br>
      <p class="shepherd-note">Work profiles help organise related research objects such as samples, reads and data files
      under a project.</p>`,
  },
  publish_record_button: {
    title: 'Publish study to ENA',
    content: `Click this button to publish the study (i.e. make the study public) to European Nucleotide Archive (ENA), 
    a public repository.<br><br>
    <strong>Submit the data first</strong> using the <button class="tiny ui basic teal button submit-btn no-click">
    <i class="fa fa-info-circle"></i>&nbsp;Submit</button> then, select <strong> one record</strong> under this
    <strong>STUDY</strong> tab in the table.<br><br>
    The publication will include all the data that relate to the selected record matching the study ID in the <strong>Study ID</strong> column.<br><br>
    <p class="shepherd-note"> A public repository is a database that stores and shares 
    research data with the global scientific community. Publishing the study means that </p>`,
  },
  publish_record_button_zenodo: {
    title: 'Publish study to Zenodo',
    content: `Click this button to make the data public in Zenodo, a public repository.<br><br>
    Select <strong>at least one record</strong> in the table first. The submission will include 
    all selected records.<br><br>
    <p class="shepherd-note"> A public repository is a database that stores and shares 
    research data with the global scientific community.</p>`,
  },
  quick_tour_title_button: {
    title: 'Page tour',
    content:
      'To explore additional controls on this page, click this button to start a guided tour of the COPO platform.',
  },
  release_profile: {
    title: 'Release profile',
    content: `Now that you have made submissions, refer to this profile on the <strong>Work Profiles</strong> page to release it.<br><br>
    Releasing a profile makes all the submissions under the profile public and visible in public repositories like European Nucleotide Archive (ENA).<br><br>
    Refer to <a href="https://copo-docs.readthedocs.io/en/latest/profile/releasing-profiles.html#releasing-profiles-studies" target="_blank">Releasing Profiles (Studies) documentation</a> for
    detailed instructions on how to release a profile.<br><br>
    <p class="shepherd-note">A profile is also known as a study or project. Releasing a profile is an important step in finalising your data submissions
    and ensuring data integrity.</p>`,
  },
  select_all_button: {
    title: 'Select all',
    content: `Click this button to select all rows in the table.<br><br>
    This is useful for performing bulk actions on multiple items at once.`,
  },
  select_filtered_button: {
    title: 'Select filtered rows',
    content: `Click this button to select only the rows that match the current filter criteria.<br><br>
    This allows you to focus on specific subsets of data for bulk actions.`,
  },
  select_visible_button: {
    title: 'Select visible rows',
    content: `Click this button to select only the rows that are currently visible in the table.<br><br>
    This is useful for performing actions on items that you can see without affecting those that are hidden.`,
  },
  settings_icon: {
    title: 'Settings',
    content:
      'Access: <ul><li>Work profile groups</li><li>Account details (such as ORCID iD)</li><li>Logout option</li></ul>',
  },
  submit_record_button: {
    title: 'Submit data',
    content: `Click this button to submit data to European Nucleotide Archive (ENA), 
    a public repository.<br><br>
    Select <strong>at least one record</strong> in the table first. The submission will include 
    all selected records.<br><br>
    <p class="shepherd-note"> A public repository is a database that stores and shares 
    research data with the global scientific community.</p>`,
  },
  submit_record_button_zenodo: {
    title: 'Submit data',
    content: `Click this button to submit data to Zenodo, a public repository.<br><br>
    Select <strong>at least one record</strong> in the table first. The submission will include 
    all selected records.<br><br>
    <p class="shepherd-note"> A public repository is a database that stores and shares 
    research data with the global scientific community.</p>`,
  },
  view_images_button: {
    title: 'View images',
    content: `Select <strong>at least one record</strong> in the table first. Then, click this 
    button to view images associated with the uploaded data if available.<br><br>
    This action will then open a modal displaying the image(s) in a gallery format.<br><br>
    <p class="shepherd-note">Selecting no images then, clicking this button will cause an error.</p>`,
  },
};
