from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout,  Row, Column

from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import (
    SEQUENCE_ANNOTATION_FILE_TYPE_EXTS, SEQUENCE_ANNOTATION_TYPES, SEQUENCE_ANNOTATION_TYPES_MAP,
)

class AnnotationForm(forms.Form):

    def __init__(self, *args, sample_accession=None, study_accession=None, run_accession=None, experiment_accession=None, seq_annotation=None,
                 assembly_accession_choices=None, pending_assembly_names=None, **kwargs):
        super(AnnotationForm, self).__init__(initial=seq_annotation, *args, **kwargs)
        self.fields['primary_id'].choices = [('', 'Select an assembly')] + list(assembly_accession_choices or [])
        
        if pending_assembly_names:
            self.fields['primary_id'].help_text = (
                'Waiting for ENA to assign a GCA accession: ' + ', '.join(pending_assembly_names)
                + '. Assemblies can be annotated once they have one, which can take a few days.')
        elif not assembly_accession_choices:
            self.fields['primary_id'].help_text = (
                'No assemblies with a GCA accession were found. Submit the assembly first; '
                'ENA assigns its GCA accession a few days after submission.')

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
    
    title = forms.CharField(label="ANNOTATION TITLE", widget=forms.TextInput(attrs={'placeholder': 'Title of the annotation'}))

    annotation_type = forms.ChoiceField(label="ANNOTATION TYPE",
                    choices=[('sequence_annotation', 'Sequence annotation'), 
                        ('decoupled_annotation', 'Decoupled annotation')])
        
    description = forms.CharField(label="DESCRIPTION", required=False,
                                  widget=forms.Textarea(attrs={'placeholder': 'Free text description of the annotation'}))    
        
    # Primary ID choices are set in the `__init__` function above from the profile's assemblies
    primary_id = forms.ChoiceField(label="PRIMARY ID", required=False)
        
    sample = forms.ChoiceField(label="SAMPLE", required=False)
    run = forms.MultipleChoiceField(label="RUN", required=False)
    experiment = forms.MultipleChoiceField(label="EXPERIMENT", required=False)                                                                                                   

    
    id = forms.CharField(label="ID", required=False, widget=forms.HiddenInput)
    #files = forms.CharField(label="FILES", required=True, widget=forms.Textarea(attrs={'placeholder': 'Comma separated list of file names'}))
    
    
    def clean(self):
        cleaned_data = super().clean()
        sample_field = cleaned_data.get('sample')
        annotation_type =  cleaned_data.get('annotation_type')
        error = {}

        if annotation_type == 'sequence_annotation':
            # Primary ID is not needed for `Sequence annotations`; 
            # only sample is a mandatory field (a part from study)
            cleaned_data['primary_id'] = '' # Clear `primary_id` for sequence annotations
            
            if not sample_field:
                error.update({'sample': f"Please provide 'sample' for 'Sequence annotation' annotations"})
        elif annotation_type == 'decoupled_annotation':
            # Sample, run and experiment are not needed for `Decoupled annotations`; 
            # they are needed for `Sequence annotation` types. Their fields are 
            # hidden via Javascript but their values should also be cleared.
            cleaned_data['sample'] = ''
            cleaned_data['run'] = []
            cleaned_data['experiment'] = []
    
            for field in ['study', 'primary_id', 'description']:
                if not cleaned_data.get(field, ''):
                    error.update({field: f"Please provide a '{field}' for 'Decoupled annotation' annotations"})
        if error:
            raise forms.ValidationError(error)
        
        return cleaned_data
        
class AnnotationFilesForm(forms.Form):
    def __init__(self,  *args, ecs_files=None, annotation_type='sequence_annotation', **kwargs):
        super(AnnotationFilesForm, self).__init__(*args, **kwargs)
        
        self.fields['type'].initial = None
        self.fields['file'].initial = None
        self.fields['file'].widget.attrs['readonly'] = True  
        
        # Filter the ECS files to include only those with valid extensions for the selected annotation type
        # The same thing goes for the annotation type field, which should only include valid types for the selected annotation type
        if annotation_type in SEQUENCE_ANNOTATION_TYPES:
            valid_file_extensions = tuple(SEQUENCE_ANNOTATION_TYPES_MAP[annotation_type]['valid_file_extensions'])
            self.fields['file'].choices = [('', 'None')] + [
                (x, x) for x in (ecs_files or []) if x.endswith(valid_file_extensions)
            ]
            self.fields['type'].choices = [('', 'None')] + SEQUENCE_ANNOTATION_TYPES_MAP[annotation_type]['valid_file_types']
       
        self.helper = FormHelper()
        self.helper.layout = Layout(
            Row(
                Column('file', css_class='form-group col-md-8 mb-0'),
                Column('type', css_class='form-group col-md-4 mb-0'),
                css_class='form-row'
            )
        )

    file = forms.ChoiceField(label="FILE", required=True)
    type = forms.ChoiceField(label="TYPE", required=True)

    def clean(self):
        # Verify that the selected file matches its chosen file type
        # e.g. a 'gff' file must end in .gff or .gff.gz.
        cleaned_data = super().clean()
        file_name = cleaned_data.get('file')
        file_type = cleaned_data.get('type')

        if file_name and file_type in SEQUENCE_ANNOTATION_FILE_TYPE_EXTS:
            valid_exts = SEQUENCE_ANNOTATION_FILE_TYPE_EXTS[file_type]
            if not file_name.endswith(tuple(valid_exts)):
                self.add_error('file', f"'{file_name}' does not match its file type '{file_type}'. "
                    f"Expected a file ending in {', '.join(valid_exts)}")
        return cleaned_data