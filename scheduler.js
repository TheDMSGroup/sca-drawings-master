const database = require('/opt/database'),
    AWS = require('aws-sdk'),
    { DateTime } = require('luxon');

const SQS_QUEUE = 'https://sqs.us-east-1.amazonaws.com/499933439604/sca-drawing-queue';

exports.handler = async (event, context, callback) => {
    let dbh = await database.connect('aramis');

    //this script will run weekly, which means that monthly/quarterly/annual drawings will need to be performed the first week, after the conclusion
    //Do this by running when today.day <= 7
    let today = DateTime.now().setZone('America/New_York');
    let sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

    await dbh.query("select utm_content, frequency, start_date, dedupe_on_email, consumer_pathways_contest_id, timezone, site from aramis.consumer_pathways_contests where status or end_date > curdate() - interval 7 day;")
        .then(res => {
            let exports = [];
            for (let i = 0; i < res.length; i++) {
                if (res[i]['frequency'] == 'daily') {
                    for (let d = 1; d <= 7; d++) {
                        let start = DateTime.now().setZone('America/New_York').minus({ day: d });
                        let end = DateTime.now().setZone('America/New_York').minus({ day: d });

                        if (start >= res[i]['start_date']) {
                            exports.push(queueExport(sqs, res[i], start.toISODate(), end.toISODate()));
                        }
                    }
                }
                else {
                    let start, end;
                    if (res[i]['frequency'] == 'weekly') {
                        //Weekly being defined Monday to Sunday
                        start = DateTime.now().setZone('America/New_York').set({ weekday: 1 }).minus({ day: 7 }).toISODate();
                        end = DateTime.now().setZone('America/New_York').set({ weekday: 7 }).minus({ day: 7 }).toISODate();
                    }
                    else if (res[i]['frequency'] == 'monthly' && today.day <= 7) {
                        start = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ month: 1 }).toISODate();
                        end = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ day: 1 }).toISODate();
                    }
                    else if (res[i]['frequency'] == 'quarterly' && today.day <= 7 && today.month % 3 == 1) {
                        start = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ month: 3 }).toISODate();
                        end = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ day: 1 }).toISODate();
                    }
                    else if (res[i]['frequency'] == 'annually' && today.day <= 7 && today.month == 10) {
                        //Annually is defined October 1 to September 30, for historical reasons
                        //thus, run on the first week of October, for the previous year
                        start = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ year: 1 }).toISODate();
                        end = DateTime.now().setZone('America/New_York').set({ day: 1 }).minus({ day: 1 }).toISODate();
                    }

                    if (start && end) {
                        exports.push(queueExport(sqs, res[i], start, end));
                    }
                }
            }

            return Promise.all(exports);
        })
        .finally(() => dbh.close());
}

async function queueExport(sqs, row, start, end) {
    console.log('queueing ' + row.utm_content);

    return sqs.sendMessage({
        MessageBody: JSON.stringify({
            start_date: start,
            end_date: end,
            contest_id: row.rewards_advisor_contest_id,
            utm_content: row.utm_content,
            unique_entries: row.unique_entries,
            timezone: row.timezone,
            site: row.site,
            frequency: row.frequency
        }),
        QueueUrl: SQS_QUEUE,
    }
    ).promise();
}