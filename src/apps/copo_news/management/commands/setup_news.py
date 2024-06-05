from django.core.management.base import BaseCommand
from src.apps.copo_news.models import News, NewsCategory


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "Add news categories and news articles to the database, which will be used to display news on the News page"

    def __init__(self):
        super().__init__()

    def handle(self, *args, **options):
        NewsCategory().remove_all_news_categories()
        News().remove_all_news_articles()
        self.stdout.write("Removed existing news items and categories")
        self.stdout.write("Adding news items and categories...")
        # News categories
        NewsCategory().add_news_category(name='General', description='General news', colour='#294F85')
        NewsCategory().add_news_category(name='Maintenance', description='Maintenance news', colour='#7BC422')
        NewsCategory().add_news_category(name='Release', description='Release news', colour='#F7005E')
        NewsCategory().add_news_category(name='Other', description='Other news', colour='#009c95')
        
        # # News items
        # News.add_news_article(title='Manifest version v2.5 released', content='Equally, the projects - Aquatic Symbiosis Genomics (ASG), European Reference Genome Atlas (ERGA) and Darwin Tree of Life (DTOL) - have been updated to the latest manifest version v2.5.', author='COPO Project Team', category='Release', is_news_article_active=True)
        # News.add_news_article(title='COPO maintenance', content='All COPO services were disrupted for about 2 weeks in May, 2024 due to a technical issue. Please accept our apologies for any inconvenience that the disruption may have caused.', author='COPO Project Team', category='Maintenance', is_news_article_active=True)
        # News.add_news_article(title='Continued Development', content='COPO is currently funded under Biotechnology and Biological Sciences Research Council (BBSRC), part of UK Research and Innovation, through the Cellular Genomics (CELLGEN) Grant BBS/E/ER/230001A, Data Infrastructure and Algorithms Group Grant EI-G-Data,  Decoding Biodiversity (DECODE): Development of Novel Experimental and Bioinformatic Tools for Genomic Diversity and Analysis Grant BBS/E/ER/230002A and EI - Darwin Tree of Life Grant GP182.', author='COPO Project Team', category='General', is_news_article_active=True)
        # News.add_news_article(title='Customisation of a Locus Tag', content='You can now customise a locus tag in COPO. This is possible when you create a work profile in COPO.', author='COPO Project Team', category='Other', is_news_article_active=True)

        # self.stdout.write("News items and categories have been added")
        # news_records = News.objects.all()
        # category_records = NewsCategory.objects.all()

        # for x in news_records:
        #     self.stdout.write('News item: ', x.title)

        # for i in category_records:
        #     self.stdout.write('News category: ', i.name)
