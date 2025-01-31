from django.urls import path
from . import consumers

# as_asgi() is added to fix the TypeError: __call__() missing 1 required positional argument: 'send'

websocket_urlpatterns = [
    #path('ws/submission_status/<str:profile_id>/', consumers.SubmissionConsumer),
    path('ws/sample_status/<str:profile_id>', consumers.SampleConsumer.as_asgi()),
    path('ws/dtol_status', consumers.DtolConsumer.as_asgi()),
    path('ws/s3_status/<str:uid>', consumers.s3Consumer.as_asgi()),
    path('ws/assembly_status/<str:profile_id>', consumers.assemblyConsumer.as_asgi()),
    path('ws/annotation_status/<str:profile_id>', consumers.annotationConsumer.as_asgi()),
    path('ws/read_status/<str:uid>', consumers.readConsumer.as_asgi()),
    path('ws/tagged_seq_status/<str:uid>', consumers.taggedSeqConsumer.as_asgi()),
    path('ws/singlecell_status/<str:uid>', consumers.singlecellConsumer.as_asgi())

]