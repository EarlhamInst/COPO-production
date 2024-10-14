from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout,  Row, Column

class AnnotationForm(forms.Form):

    def __init__(self, *args, sample_accession=None, study_accession=None, run_accession=None, experiment_accession=None, seq_annotation=None,  **kwargs):
        super(AnnotationForm, self).__init__(initial=seq_annotation, *args, **kwargs)
        if study_accession:
            self.fields['study'].initial = study_accession
            self.fields['study'].widget.attrs['readonly'] = True
 
        if run_accession:
            self.fields['run'].widget.attrs['readonly'] = True
            self.fields['run'].choices = [(x,x) for x in run_accession]
        if experiment_accession:
            self.fields['experiment'].widget.attrs['readonly'] = True
            self.fields['experiment'].choices =  [(x,x) for x in experiment_accession]
 
        #self.fields['sample'].choices = []
        if sample_accession:
            self.fields['sample'].widget.attrs['readonly'] = True
            self.fields['sample'].choices=[(x,x) for x in sample_accession]
        if kwargs.get('id', ""):
            self.fields['id'].initial = kwargs.get('id', "")
            self.fields['id'].widget.attrs['readonly'] = True

        if seq_annotation:
            #self.fields["study"].initial = seq_annotation.get("study", "")
            #self.fields["sample"].initial = seq_annotation.get("sample", "")
            #self.fields["run"].initial = seq_annotation.get("run", "")
            #self.fields["experiment"].initial = seq_annotation.get("experiment", "")
            #self.fields["title"].initial = seq_annotation.get("title", "")
            #self.fields["description"].initial = seq_annotation.get("description", "")
            self.fields["id"].initial =  str(seq_annotation["_id"])

    # fields from ENA annotation documentation
    study = forms.CharField(label="STUDY",
                            widget=forms.TextInput(attrs={'placeholder': 'Study accession'}))
    sample = forms.ChoiceField(label="SAMPLE")
    run = forms.MultipleChoiceField(label="RUN", required=False)
    experiment = forms.MultipleChoiceField(label="EXPERIMENT", required=False)

    title = forms.CharField(label="ANNOTATION TITLE", widget=forms.TextInput(attrs={'placeholder': 'Annotation title user-provided'}))
                                                                                                   
    description = forms.CharField(label="DESCRIPTION", required=False,
                                  widget=forms.Textarea(attrs={'placeholder': 'Free text description of the sequence annotation'
                                                                              'annotation'}))    
    id = forms.CharField(label="ID", required=False, widget=forms.HiddenInput)
    #files = forms.CharField(label="FILES", required=True, widget=forms.Textarea(attrs={'placeholder': 'Comma separated list of file names'}))
    
class AnnotationFilesForm(forms.Form):
    def __init__(self,  *args, ecs_files=None,  **kwargs):
        super(AnnotationFilesForm, self).__init__(*args, **kwargs)
        
        self.fields['type'].initial = None
        self.fields['file'].initial = None
        self.fields['file'].widget.attrs['readonly'] = True  
        files_choices = [("", 'None')]

        if ecs_files:
            files_choices.extend([(x,x) for x in ecs_files])
        self.fields['file'].choices = files_choices
       

        self.helper = FormHelper()
        self.helper.layout = Layout(
            Row(
                Column('file', css_class='form-group col-md-8 mb-0'),
                Column('type', css_class='form-group col-md-4 mb-0'),
                css_class='form-row'
            )
        )

    file = forms.ChoiceField(label="FILE", required=True )
    type = forms.ChoiceField(label="TYPE", required=True,
                                     choices=[('','None'),('gff', 'gff'), ('tab', 'tab'),
                                              ("fasta", "fasta"), ("bed", "bed")])