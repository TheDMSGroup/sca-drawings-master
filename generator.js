
const database = require('/opt/database'),
    ftp = require('basic-ftp'),
    Readable = require('stream').Readable,
    secret = require('/opt/getSecret'),
    AWS = require('aws-sdk'),
    crypto = require('crypto'),
    Cryptr = require('cryptr'),
    _ = require('lodash');

const S3_BUCKET = 'sca-submissions';
const key = 'Ar4M1S8h1Mp3455R';

exports.handler = async (event, context, callback) => {
    for (const record of event.Records) {
        if (record && record.body) {
            await produce_file(JSON.parse(record.body));
        }
    }
}

async function produce_file(record) {
    let dbh = await database.connect('aramis');
    let s3 = new AWS.S3({ apiVersion: '2006-03-01' });
    let ftp_credentials = await secret.getSecret('sca-ftp');

    console.log({ record });

    let filename = `${record['utm_content']}-${record.start_date}-to-${record.end_date}`;
    let file = '';
    let hash = '';

    const client = new ftp.Client();

    await client.access({
        host: ftp_credentials.host,
        user: ftp_credentials.user,
        password: ftp_credentials.password,
        secure: true
    });

    return get_records(dbh, record)
        .then(res => {
            file = `"Email","Timestamp","Promotion","First Name","Last Name","Address","City","State","Zip"\r\n`;
            for (let i = 0; i < res.length; i++) {
                file += `"${res[i]['email']}","${res[i]['timestamp']}","${record['utm_content']}","${res[i]['first_name']}","${res[i]['last_name']}","${res[i]['address']}","${res[i]['city']}","${res[i]['state']}","${res[i]['zip']}"\r\n`;
            }
            hash = crypto.createHash('sha256').update(file).digest('hex');
            console.log({ utm_content: record['utm_content'], hash: hash });

            return s3.putObject({ Bucket: S3_BUCKET, Key: `${filename}.txt`, Body: file }).promise();
        }).then(res => {
            return s3.putObject({ Bucket: S3_BUCKET, Key: `${filename}.hsh`, Body: hash }).promise();
        }).then(res => {
            //write users to FTP
            let read = new Readable();
            read.push(file);
            read.push(null);
            //return client.uploadFrom(read, `${filename}.txt`);
        })
        .then(res => {
            //write hash to FTP
            let read = new Readable();
            read.push(hash);
            read.push(null);
            //return client.uploadFrom(read, `${filename}.hsh`);
        })
        .catch(err => console.log(err))
        .finally(() => { dbh.close(); client.close() });
}

async function get_records(dbh, contest) {
    let cryptr = new Cryptr(key);
    return dbh.query('SET time_zone = ?', [contest.timezone])
        .then(res => {
            return dbh.query("SELECT email, date_format(datetime, '%Y-%m-%d %H:%i:%s') AS datetime, utm_content, date_format(datetime, '%Y%m%d') AS date, first_name, last_name, address1, city, state, postal_code FROM user_details USE INDEX (datetime) WHERE property_id = '420' AND datetime >= ? AND datetime < ? + INTERVAL 1 DAY AND utm_content = ? AND hasoffers_site_id = ? ", [contest.start_date, contest.end_date, contest.utm_content, contest.site])
        })
        .then(res => {
            let final_records = [];
            let staging_records = {};
            let err_ctr = 0;
            let dupe_ctr = 0;
            for (let i = 0; i < res.length; i++) {
                try {
                    let email = cryptr.decrypt(res[i]['email']);
                    let address = cryptr.decrypt(res[i]['address1']);

                    if (contest.dedupe_on_email) {
                        if (!(email in records) || res[i]['datetime'] < records[email]['timestamp']) {
                            if (res[i]['datetime'] < records[email]['timestamp']) {
                                dupe_ctr++;
                            }

                            staging_records[email] = { email: email, timestamp: res[i]['datetime'], date: res[i]['date'], first_name: res[i]['first_name'], last_name: res[i]['last_name'], address: address, city: res[i]['city'], state: res[i]['state'], zip: res[i]['postal_code'] };
                        }
                    }
                    else {
                        final_records.push({ email: email, timestamp: res[i]['datetime'], date: res[i]['date'], first_name: res[i]['first_name'], last_name: res[i]['last_name'], address: address, city: res[i]['city'], state: res[i]['state'], zip: res[i]['postal_code'] })
                    }
                } catch (err) { err_ctr++ }
            }
            console.log(contest.utm_content + ' errors: ' + err_ctr);
            console.log(contest.utm_content + ' dupes: ' + dupe_ctr);

            if (contest.dedupe_on_email) {
                final_records = _.values(staging_records);
            }

            console.log(contest.utm_content + " Good Records: " + final_records.length);
            return final_records;
        })
        .catch(err => console.log(err));
}
