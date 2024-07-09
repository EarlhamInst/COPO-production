from django.core.files.storage import FileSystemStorage

class OverwriteStorage(FileSystemStorage):

    def get_available_name(self, name, max_length=None):
        """
        Returns a filename that's free on the target storage system and
        available for new content to be written to.

        Found at http://djangosnippets.org/snippets/976/

        This file storage solves the overwrite on upload problem and solves the uploaded  
        file having a  having a unique suffix appended to it when a file with the 
        same name already exists.
        
        Another proposed solution was to override the save method on the model
        like from link - https://code.djangoproject.com/ticket/11663) 
        described below:

        def save(self, *args, **kwargs):
            try:
                this = MyModelName.objects.get(id=self.id)
                if this.MyImageFieldName != self.MyImageFieldName:
                    this.MyImageFieldName.delete()
            except: pass
            super(MyModelName, self).save(*args, **kwargs)
        """
        # If the filename already exists, 
        # remove it as if it was a true file system
        self.delete(name)
        return name