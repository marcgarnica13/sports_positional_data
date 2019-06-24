function start_monitoring() {
    window.setInterval(function () {
        var api_url = 'http://localhost:4040/api/v1/applications'
        var application_id = document.getElementById('data_file').files[0].name
        console.log(application_id)

        $.ajax({
            url: api_url,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': 'application/json'
            },
            method: 'GET',
            dataType: 'json',
            success: function (result) {
                result.forEach(function (item, index) {
                    if (item.name == 'repo_ingestion') get_jobs(item.id)
                })
            }
        })

        function get_jobs(application_id) {
            $.ajax({
                url: api_url + '/' + application_id + '/jobs',
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET',
                    'Access-Control-Allow-Headers': '*',
                    'Content-Type': 'application/json'
                },
                method: 'GET',
                dataType: 'json',
                success: function (result) {
                    console.log(result);
                    update_loading_messages(result)
                }
            })
        }

        function update_loading_messages(jobs_list) {
            var jobs = []
            jobs_list.forEach(function (item, index) {
                if (item.jobGroup.split('#')[0] == application_id) {
                    jobs.push(
                        return_new_progress(item.jobGroup.split('#')[1], item.jobGroup.split('#')[2], item.status, item.submissionTime, item.numTasks, item.numCompletedTasks)
                    )
                }

            })
            $('#jobs_list').html(jobs.join(''));
        }

        function return_new_progress(jobGroup, jobDescription, jobStatus, submissionTime, numTasks, taskCompleted) {
            return '<div class="project">'+
                    '<div class="row bg-white has-shadow">'+
                        '<div class="left-col col-lg-6 d-flex align-items-center justify-content-between">' +
                            '<div class="project-title d-flex align-items-center">'+
                                '<div class="text">'+
                                    '<h3 class="h4">' + jobGroup + '</h3><small>' + jobDescription + '</small>'+
                                '</div>'+
                                '</div>'+
                                '<div class="project-date"><span class="hidden-sm-down">Submitted ' + extract_date(submissionTime) + '</span></div>'+
                              '</div>'+
                              '<div class="right-col col-lg-6 d-flex align-items-center justify-content-between">'+
                                '<div class="comments"><i class="fa fa-comment-o"></i>' +taskCompleted + ' of ' + numTasks + ' tasks completed</div>'+
                                '<div class="project-progress">'+
                                  '<div class="progress">'+
                                    '<div role="progressbar" style="width: ' + ((taskCompleted/numTasks) * 100) + '%; height: 6px;" aria-valuemin="0" aria-valuemax="100" class="progress-bar ' + get_progress_bar_color(jobStatus) + '"></div>'+
                                  '</div>'+
                             '</div>'+
                            '</div>'+
                           '</div>'+
                          '</div>'
        }

        function get_progress_bar_color(status) {
            switch (status) {
                case "SUCCEEDED":
                    return "bg-green";
                case "RUNNING":
                    return "bg-blue";
                case "FAILED":
                    return "bg-red";
                case "UNKOWN":
                    return "bg-yellow";
                default:
                    return "bg-blue";
            }
        }

        function extract_date(time) {
            console.log(time)
            return moment(time, 'YYYY-MM-DDTHH:mm:ssZ').add(2, 'hours').fromNow();
        }
    }, 1000);
}