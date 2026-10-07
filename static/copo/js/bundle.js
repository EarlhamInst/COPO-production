$(document).ready(function(){

    $('#submission_type').change(function(){

        var s_type = $('#submission_type').val();

        $.ajax({
            type:"GET",
            url:"/rest/test/",
            async:false,
            dataType:"json",
            success:form_done,
            error:function(){
                //alert('no json returned')
            },
            data:{submission_type:s_type}
        });
    });

    function form_done(data){
        //for each element in returned data, append a new html control to the form
        var theForm = $('#ena_study_form');
        var controls = $(jQuery.parseJSON(data));

        for(i = 0; i < controls.length; i++){
            var field_name = controls[i].name;
            var field_type = controls[i].type;
            var meta = controls[i].meta;
            if(field_type == 'text'){
                if(meta == 'many'){
                    //append a label
                    theForm.append("<br/><label for='" + field_name + "'>" + field_name + "</label>");
                    //append the control
                    theForm.append("<input type='text' class='form-control' name=" + field_name + "></input>");
                    theForm.append("<button type='button' class='btn btn-info btn-xs add_many_button'>add another</button>");
                    theForm.append("<hr/>")
                }
                else if(meta == 'one'){
                    //append a label
                    theForm.append("<br/><label for='" + field_name + "'>" + field_name + "</label>");
                    //append the control
                    theForm.append("<input type='text' class='form-control' name=" + field_name + "></input>")
                }
            }

        }
    }


});