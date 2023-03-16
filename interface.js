const database = require('/opt/database'),
    qs = require('qs'),
    _ = require('lodash'),
    { DateTime } = require('luxon');

const ALLOWED_RULESETS = ['gift-card', 'brand-v-brand', 'big-cash', 'daily-sweeps'];
const ALLOWED_SITES = ['2017', '1311'];
const ALLOWED_FREQUENCIES = ['daily', 'weekly', 'monthly', 'quarterly', 'annually'];
const ALLOWED_TIMEZONES = ['America/New_York', 'America/Los_Angeles', 'UTC'];

exports.handler = async (event, context, callback) => {
    let params = parseParameters(event);

    if (params['action'] == 'get_promotions') {
        let dbh = await database.connect();
        return dbh.query("select consumer_pathways_contest_id, utm_content, human_readable_name, ruleset, frequency, dedupe_on_email, status, timezone, site, start_date, end_date from consumer_pathways_contests where (status or end_date > curdate());")
            .then(res => {
                for (let i = 0; i < res.length; i++) {
                    if (res[i]['start_date']) {
                        res[i]['start_date'] = res[i]['start_date'].toISOString().slice(0, 10);
                    }
                    if (res[i]['end_date']) {
                        res[i]['end_date'] = res[i]['end_date'].toISOString().slice(0, 10);
                    }
                }
                console.log(res[0]);
                return {
                    statusCode: 200,
                    body: JSON.stringify({ promotions: res })
                };
            })
            .catch(err => console.log(err))
            .finally(() => dbh.close());
    }
    else if (params['action'] == 'add_promotion') {
        if (!ALLOWED_SITES.includes(params.site)) {
            return {
                statusCode: 400,
                body: 'Invalid Site',
            };
        }
        if (!ALLOWED_RULESETS.includes(params.ruleset)) {
            return {
                statusCode: 400,
                body: 'Invalid Ruleset',
            };
        }
        if (!ALLOWED_FREQUENCIES.includes(params.frequency)) {
            return {
                statusCode: 400,
                body: 'Invalid Frequency',
            };
        }
        if (!ALLOWED_TIMEZONES.includes(params.timezone)) {
            return {
                statusCode: 400,
                body: 'Invalid Timezone',
            };
        }

        //find the first valid start date on or after the given start date
        let start_date = DateTime.fromISO(params.start_date);
        if (params.frequency == 'weekly') {
            if (start_date.weekday != 1) {
                start_date = start_date.plus({ 'days': 8 - start_date.weekday })
            }
        }
        else if (params.frequency == 'monthly') {
            if (start_date.get('day') != 1) {
                start_date = start_date.plus({ 'months': 1 }).set({ 'day': 1 });
            }
        }
        else if (params.frequency == 'quarterly') {
            if (start_date.get('day') != 1) {
                start_date = start_date.plus({ 'months': 1 }).set({ 'day': 1 });
            }
            if (start_date.get('month') % 3 == 0) {
                start_date = start_date.plus({ "months": 1 })
            }
            else if (start_date.get('month') % 3 == 2) {
                start_date = start_date.plus({ "months": 2 })
            }
        }
        else if (params.frequency == 'annually') {
            if (!(start_date.get('month') == 10 && start_date.get('day') == 1)) {
                if (start_date.get('month') >= 10) {
                    start_date = start_date.plus({ 'years': 1 })
                }
                start_date = start_date.set({ 'month': 10, 'day': 1 });
            }
        }

        let dbh = await database.connect();
        return dbh.query("INSERT INTO consumer_pathways_contests (utm_content, human_readable_name, ruleset, frequency, dedupe_on_email, timezone, site, start_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [params.utm_content, params.name, params.ruleset, params.frequency, params.dedupe, params.timezone, params.site, start_date.toISODate()])
            .then(res => {
                return {
                    statusCode: 200,
                    body: JSON.stringify({ promotions: res })
                };
            })
            .catch(err => console.log(err))
            .finally(() => dbh.close());
    }
    else if (params['action'] == 'end_promotion') {
        console.log({ params });

        let dbh = await database.connect();
        return dbh.query("SELECT frequency FROM consumer_pathways_contests WHERE consumer_pathways_contest_id = ?", [params.contest_id])
            .then(res => {

                if (res.length != 1) {
                    throw "Invalid Contest ID";
                }
                let frequency = res[0].frequency;
                //find the first valid start date on or after the given start date
                let end_date = DateTime.fromISO(params.end_date);
                if (frequency == 'weekly') {
                    if (end_date.weekday != 7) {
                        end_date = end_date.plus({ 'days': 7 - end_date.weekday })
                    }
                }
                else if (frequency == 'monthly') {
                    end_date = end_date.plus({ 'months': 1 }).set({ 'day': 1 }).minus({ "days": 1 });
                }
                else if (frequency == 'quarterly') {
                    end_date = end_date.plus({ 'months': 1 }).set({ 'day': 1 });
                    if (end_date.get('month') % 3 == 0) {
                        end_date = end_date.plus({ "months": 1 })
                    }
                    else if (end_date.get('month') % 3 == 2) {
                        end_date = end_date.plus({ "months": 2 })
                    }
                    end_date = end_date.minus({ 'days': 1 })
                }
                else if (frequency == 'annually') {
                    if (!(end_date.get('month') == 10 && end_date.get('day') == 1)) {
                        if (end_date.get('month') >= 10) {
                            end_date = end_date.plus({ 'years': 1 })
                        }
                        end_date = end_date.set({ 'month': 10, 'day': 1 });
                    }
                    end_date = end_date.minus({ "days": 1 })
                }

                return dbh.query("UPDATE consumer_pathways_contests SET status = 0, end_date = ? WHERE consumer_pathways_contest_id = ?", [end_date.toISODate(), params['consumer_pathways_contest_id']])
            }).then(res => {
                return {
                    statusCode: 200,
                    body: JSON.stringify({ promotions: res })
                };
            })
            .catch(err => {
                console.log(err);
                return {
                    statusCode: 400,
                    body: res
                };
            })
            .finally(() => dbh.close());
    }
    else {
        return {
            statusCode: 400,
            body: 'Invalid Action',
        };
    }
}


function parseParameters(event) {
    let params;
    if (event.queryStringParameters) {
        params = event.queryStringParameters;
        if (!params.user_ip) {
            params.user_ip = event.headers['X-Forwarded-For'];
        }
        if (!params.user_agent) {
            params.user_agent = event.headers['User-Agent'];
        }
    }
    else if (event.body) {
        let body = event.body;
        if (event.isBase64Encoded) {
            let buff = new Buffer(body, 'base64');
            body = buff.toString('ascii');
            console.log({ body });
        }
        params = qs.parse(body);
    }
    return params;
}