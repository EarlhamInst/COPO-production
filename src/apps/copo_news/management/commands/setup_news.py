from django.core.files import File
from django.core.management.base import BaseCommand
from src.apps.copo_news.models import News, NewsCategory
import os

'''
◉ To solve the permission issues and the ability for Django to write images to 
  the 'news_images' directory in the 'media' folder, ensure that the 'news_images'
  directory has the correct permissions and ownership
   
  $ sudo chmod -R 775 /media/news_images                 # Change permissions to allow write access
  $ sudo chown -R www-data:www-data /media/news_images      # Change ownership to the user who runs the Django management command 
                                                           (typically your user or www-data)

   NB: where 'www-data' is your user as well as group 
        '/media/news_images' and its subdirectories are where the images are stored

......................................................

◉  Check the permissions of the 'news_images' directory in the 'media' directory:
   $ ls -ld /media/news_images
'''

# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Add news categories and news articles to the database, which will be used to display news on the News page'

    def handle(self, *args, **options):
        # Clear existing data
        NewsCategory().remove_all_news_categories()
        News().remove_all_news_articles()

        self.stdout.write(self.style.SUCCESS('Removed existing news items and categories\n'))
        
        # Ensure 'news_images' directory exists
        self.stdout.write('Checking if \'news images\' directory...\n')
        News().create_news_images_directory()

        # Set the correct permissions for the 'news_images' directory
        self.stdout.write('Setting folder permissions...')
        News().set_news_images_directory_permissions()

        self.stdout.write('Adding news items and categories...')

        # Add news categories
        categories = [
            {'name': 'General', 'description': 'General news', 'colour': '#294F85'},
            {'name': 'Maintenance', 'description': 'Maintenance news', 'colour': '#7BC422'},
            {'name': 'Feature', 'description': 'Feature news', 'colour': '#F7005E'},
            {'name': 'Other', 'description': 'Other news', 'colour': '#009c95'},
        ]

        for category in categories:
            NewsCategory.objects.create(**category)

        # Add news items
        news_items = [
            {
                'title': 'Maintenance of COPO Project\'s Website',
                'content': '<p>All Collaborative OPen Omics (COPO) services were disrupted for about 2 weeks in May, 2024 due to a technical issue.</p>'
                           '<p>Please accept our apologies for any inconvenience that the disruption may have caused.</p>',
                'category': NewsCategory.objects.get(name='Maintenance'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/maintenance.png',
                'is_news_article_active': True
            },
            {
                'title': 'Manifest version v2.5 Release',
                'content': 'Equally, the projects - Aquatic Symbiosis Genomics (ASG), European Reference Genome Atlas (ERGA) and Darwin Tree of Life (DToL), which are brokered through COPO, have been updated to the latest manifest version 2.5.',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/release_image.png',
                'is_news_article_active': True
            },
            {
                'title': 'Continued Website Development and User Support',
                'content': 'The Collaborative OPen Omics (COPO) project is currently being funded by the following: <ul><li>Biotechnology and Biological Sciences Research Council (BBSRC), part of UK Research and Innovation, through the Cellular Genomics (CELLGEN) Grant BBS/E/ER/230001A</li><li>Data Infrastructure and Algorithms Group Grant EI-G-Data</li> <li>Decoding Biodiversity (DECODE): Development of Novel Experimental and Bioinformatic Tools for Genomic Diversity and Analysis Grant BBS/E/ER/230002A</li><li>EI - Darwin Tree of Life Grant GP182</li></ul>',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/continued_development_image.jpg',
                'is_news_article_active': True
            },
            {
                'title': 'Customisation of Locus Tags for Data Submission',
                'content': '<p>You can now customise a locus tag on the Collaborative OPen Omics (COPO) platform. Locus tags, according to the <a href="https://ena-docs.readthedocs.io/en/latest/faq/locus_tags.html#what-are-locus-tags" target="_blank">definition</a> by European Nucleotide Archive (ENA), are identifiers applied systematically to every gene in a sequencing project.</p>'
                           '<p>The customisation of locus tags in COPO is possible when you create a <a href="https://copo-docs.readthedocs.io/en/latest/help/glossary.html#term-COPO-profile" target="_blank">work profile</a>.</p>',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/countryside.jpg',
                'is_news_article_active': True
            },
            {
                'title': 'Comprehensive Documentation for COPO Now Available',
                'content': '<p>The Collaborative OPen Omics (COPO) project now has extensive documentation aimed at enhancing user experience, website usability and accessibility.</p>'
                           '<p>The documentation covers all aspects of COPO\'s functionalities, from metadata submissions, data file submissions, permit submissions and image submissions, to advanced features, providing a valuable resource for researchers and scientists in the field of biodiversity. Users can access step-by-step guides, visual aides and frequently asked questions, making it easier than ever to leverage COPO\'s powerful metadata brokering platform for their research need.</p>'
                           '<p>Explore the <a href="https://copo-docs.readthedocs.io/en/latest" target="_blank">documentation</a> today and discover how COPO can streamline your metadata, data files and sharing processes.</p>',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/copo_docs_logo.png',
                'is_news_article_active': True
            },
            {
                'title': 'Celebrating a Decade of Innovation: COPO Marks its 10-Year Anniversary',
                'content': '<p>The Collaborative OPen Omics (COPO) project is celebrating its 10-year anniversary on 14th September, 2024, marking a decade of advancements in the field of biodiversity research.</p> <p>Over the past ten years, COPO has become an indispensable tool for researchers worldwide, facilitating data sharing and collaboration across institutions.</p>'
                          '<p>To commemorate this milestone, COPO might host events TBD, reflecting on its achievements and discussing future directions.</p> <p>Join us in celebrating ten years of scientific innovation and collaboration!</p>',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/anniversary_10th.png',
                'is_news_article_active': True
            },
            {
                'title': 'COPO Enhances User Support with New Frequently Asked Questions Section',
                'content': '<p>In an effort to improve user support and accessibility, the Collaborative OPen Omics (COPO) initiative has launched a comprehensive <a href="https://copo-docs.readthedocs.io/en/latest/help/faq.html" target="_blank">Frequently Asked Questions (FAQ)</a> section on its website.</p>'
                           '<p>This new resource addresses common queries and provides detailed answers on topics ranging from account setup to data submission protocols. By offering clear and concise information, COPO aims to streamline the user experience, ensuring that researchers can efficiently utilise the platform\'s full range of features.</p>',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/faq.png',
                'is_news_article_active': True
            },
            {
                'title': 'COPO Revolutionises Research with Innovative Publications',
                'content': 'The Collaborative OPen Omics (COPO) project continues to lead the way in research dissemination by publishing groundbreaking papers that enhance the scientific community\'s understanding of various biological processes. These publications not only offer new insights but also set a new standard for research excellence, ensuring that the latest scientific discoveries are shared promptly and widely.',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/publications.jpg',
                'is_news_article_active': True
            },
             {
                'title': 'COPO Streamlines Files Submission with User-Friendly Process',
                'content': 'Simplifying the complexities of data submission, COPO introduces an intuitive files submission process designed to save researchers time and effort. This new system ensures that all data is uploaded accurately and efficiently, providing a seamless experience that allows scientists to focus more on their research and less on administrative tasks.',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/redadmiral.jpg',
                'is_news_article_active': True
            },
             {
                'title': 'COPO Successfully Brokers Major Projects such as ASG, DToL, ERGA and Genomics',
                'content': 'Demonstrating its pivotal role in the scientific community, the Collaborative OPen Omics (COPO) platform has brokered significant projects such as Aquatic Symbiosis Genomics (ASG), Darwin Tree of Life (DToL), European Reference Genome Atlas (ERGA) and various genomics metadata. These projects, supported by COPO\'s robust infrastructure and expertise, promise to drive forward crucial research and collaboration efforts, pushing the boundaries of life science and biodiversity.',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/copo_logo_new.png',
                'is_news_article_active': True
            },
             {
                'title': 'COPO Achieves Milestone with Nearly 60,000 Samples Brokered',
                'content': 'As of 3rd July, 2024, the Collaborative OPen Omics (COPO) project has successfully brokered a remarkable 59,521 samples, underscoring its role as a key player in the research community. This milestone reflects COPO\'s commitment to facilitating extensive scientific studies and collaborations, enabling researchers worldwide to access and share invaluable data for groundbreaking discoveries.',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/copo_logo_new.png',
                'is_news_article_active': True
            },
            {
                'title': 'COPO Welcomes New Project Group Leader',
                'content': '<p>We are excited to announce the appointment of our new project group leader, <a href="https://www.earlham.ac.uk/profile/irene-papatheodorou" target="_blank">Professor Irene Papatheodorou</a>. Irene will be leading a group of skilled data-led scientists as the Head of Data Science at the <a href="https://www.earlham.ac.uk" target="_blank">Earlham Institute</a>, in Norwich, United Kingdom.</p>'
                           '<p>With a wealth of experience in single cell transcriptomics, bioinformatics and leadership, Irene will drive forward COPO\'s mission to enhance single-cell genomics, spatial transcriptomics and other research efforts.</p>'
                           '<p>Stay tuned for more updates on the innovative projects and initiatives under her leadership.</p>',
                'category': NewsCategory.objects.get(name='General'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/irene.jpg',
                'is_news_article_active': True
            },
            {
                'title': 'Track Your ENA File Processing Status with COPO',
                'content': '<p>Researchers, metadata providers and metadata submitters can now seamlessly track the status of data file submissions after COPO deposits them to <a href="https://www.ebi.ac.uk/ena/browser/home" target="_blank">European Nucleotide Archive (ENA)</a> after the data files have been submitted through COPO.</p>'
                           '<p>After having uploaded data files and completed metadata submissions in COPO, stay informed with real-time updates on the processing progress. This new feature enhances transparency and ensures that you are always in the loop about your data file submissions.</p>'
                           '<p>See our <a href="https://copo-docs.readthedocs.io/en/latest/submissions/files.html#checking-ena-file-upload-status" target="_blank">documentation</a> for more information on how to track the data file processing status.</p>',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/file.png',
                'is_news_article_active': True
            },
            {
                'title': 'COPO Enhances Tree of Life Projects with Expanded Capabilities',
                'content': '<p>Tree of Life (ToL) projects brokered through COPO can now process reads, sequencing annotations and assemblies more efficiently than ever.</p>' 
                           '<p>This advancement supports comprehensive analysis and accelerates research timelines, providing researchers with robust tools to delve deeper into life science and biodiversity research.</p>'
                           '<p> See our <a href="https://copo-docs.readthedocs.io/en/latest/profile/tol/tol-profile-walkthrough.html" target="_blank">guidelines</a> about how to create a Tree of Life profile in order to submit your desired research object through COPO.</p>',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/copo_logo_new.png',
                'is_news_article_active': True
            },
            {
                'title': 'Access Submission Accession Numbers Easily with COPO',
                'content': 'COPO now allows researchers to view accession numbers for their submissions, streamlining the process for analysis and publications. '
                           'These accession numbers are critical for referencing in scholarly articles and ensuring reproducibility in research, enhancing the integrity and visibility of your work.',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/accessions_component.png',
                'is_news_article_active': True
            },
            {
                'title': 'Introducing the New COPO Website: Refactored, Redesigned and Revamped',
                'content': '<p>We are thrilled to unveil the newly refactored and redesigned COPO project website, complete with a fresh logo and an updated codebase. The revamped site offers an improved user experience, featuring a sleek design and enhanced functionality.</p>'
                           '<p>Explore the new features and discover how COPO continues to support the scientific community with cutting-edge tools and resources.</p>',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/new.png',
                'is_news_article_active': True
            },
            {
                'title': 'COPO Integrates RO-Crate for Streamlined Research Data Management',
                'content': '<p>The COPO (Collaborative OPen Omics) platform has taken a significant step forward by integrating Research Object Crate (RO-Crate) into its metadata brokering platform for all submitted samples. This integration aims to enhance the organisation, packaging, and sharing of research data, providing a more efficient and standardised approach for researchers and data stewards alike.</p>'
                           '<p>RO-Crate is a community-driven initiative designed to establish a lightweight, flexible method for packaging research data along with their metadata. At its core, an RO-Crate is a Research Object (RO) that comprises a collection of data files and a ro-crate-metadata.json file, which provides a detailed description of the collection. This collection can include a diverse range of research materials such as papers, data files, software, and references to other research works. Whether it\'s a folder filled with files or an abstract grouping of connected references, RO-Crate accommodates various data types and structures.</p>'
                           '<p>The integration of RO-Crate into COPO is set to benefit both producers and consumers of research data. The RO-Crate community brings together practitioners from diverse backgrounds with varying motivations and use cases. Core target users include:</p>'
                           '<ul><li>Researchers involved in computation and data-intensive, workflow-driven analysis.</li><li>Digital repository managers and infrastructure providers.</li><li>Individual researchers seeking straightforward tools or guides to “FAIRify” their data.</li><li>Data stewards supporting research projects in creating and curating datasets.</ul>'
                           '<p>One of the key features of this integration is the ability to return sample metadata in the RO-Crate format using the `/manifest/{manifest_id}` COPO API method. This functionality can be accessed through the <a href="https://copo-project.org/api" target="_blank">COPO API web page</a>, available  <a href="https://copo-project.org/static/swagger/apidocs_index.html#/Manifest/get_manifest__manifest_id_" target="_blank">here</a>.'
                           '<p>With this enhancement, COPO continues to advance its mission of facilitating open and  Findable, Accessible, Interoperable, Reusable (FAIR) data practices, fostering greater collaboration and innovation in the research community.',
                'category': NewsCategory.objects.get(name='Feature'),
                'author': 'COPO Project Team',
                'news_image': '/copo/static/assets/img/copo_and_rocrate_logos.png',
                'is_news_article_active': True
            },
            #{
            #     'title': '',
            #     'content': '',
            #     'category': NewsCategory.objects.get(name='General'),
            #     'author': 'COPO Project Team',
            #     'news_image': '',
            #     'is_news_article_active': True
            # },
       ]

        # Create a mapping between the news image title and the news item image from the news_items list
        # This is to ensure that the image path is saved in the database according to the news item model ID
        image_title_to_news_item_mapping = {news_item['title']: news_item['news_image'] for news_item in news_items}
        
        try: 
            for news_item in news_items:
                news_image_path = news_item.get('news_image','')
                
                if not os.path.exists(news_image_path):
                    self.stdout.write(self.style.ERROR(f'Error: Image file does not exist: {news_image_path}' ))
                    continue  # Skip this news item
                try: 
                    with open(news_image_path, 'rb') as image_file:
                        django_file = File(image_file)
                        news_item_instance = News(**news_item)
                        news_item_instance.news_image.save(os.path.basename(news_image_path), django_file, save=True)
                        news_item_instance.save()
                except FileNotFoundError:
                   self.stdout.write(self.style.ERROR(f'File not found: {news_image_path}' ))
                except PermissionError:
                    self.stdout.write(self.style.ERROR(f'Permission denied: {news_image_path}' ))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'An error occurred: {e}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
            return

        # Update image path for all news items so that the 
        # image path is saved in the  database according to 
        # the news item model ID
        News().update_image_path_for_all_objects(image_title_to_news_item_mapping)
            
        self.stdout.write(self.style.SUCCESS('News items and categories have been added'))

        # Display added records
        category_records = NewsCategory.objects.all()
        news_records = News.objects.all()
     
        for i in category_records:
            self.stdout.write(f'News category: {i.name}')

        print('\n_______________\n')

        for x in news_records:
            self.stdout.write(f'News item: {x.title}; Image URL: {x.news_image.url}')