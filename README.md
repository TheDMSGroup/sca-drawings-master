# sca-drawings-masterRewards Advisor (and potentially other sites in the future) run drawings with random users winning gift cards or other prizes.  Drawings are managed by a third party, SCA.

Contest entry is defined by utm_content parameter in the inbound url

User entries are retrieved from the compliance database, aramis.user_details

Contests are defined in the table aramis.consumer_pathways_contests


CREATE TABLE `consumer_pathways_contests` (
  `consumer_pathways_contest_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `utm_content` varchar(256) DEFAULT NULL,
  `human_readable_name` varchar(256) DEFAULT NULL,
  `ruleset` enum('giftcard','brandvsbrand','bigcash','dailysweeps','gift-card','brand-v-brand','daily-sweeps','big-cash') DEFAULT NULL,
  `frequency` enum('daily','weekly','monthly','quarterly','annually') DEFAULT NULL COMMENT 'annually defined as October 1 to September 30',
  `start_date` date DEFAULT NULL,
  `dedupe_on_email` tinyint(1) DEFAULT 0,
  `creation_date` timestamp NOT NULL DEFAULT current_timestamp(),
  `end_date` date DEFAULT NULL,
  `status` tinyint(1) DEFAULT 1,
  `timezone` varchar(64) DEFAULT 'America/New_York',
  `site` varchar(32) DEFAULT '2017',
  PRIMARY KEY (`consumer_pathways_contest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

Lambda script sca-drawings-master-scheduler runs weekly on Monday

Script selects all contests that ended within the past 7 days

Script calculates appropriate start and end dates for each contest.  

For daily contests, this results in the scheduler queuing 7 files

Script queues file generation for each contest to SQS queue sca-drawing-queue

Message includes utm_content, start and end dates, timezone, site, and dedupe information

Lambda script sca-drawings-master-generator is triggered by entries in the above SQS queue

Script pulls entries based on the requested file information

Entries are submitted in csv format

filename has the pattern of ${record['utm_content']}-${record.start_date}-to-${record.end_date}.txt

(yes, I know I said csv format and have a .txt extension)

A sha256 Hash is calculated for the file for integrity, and submitted with the same filename, with a .hsh extension

Files are submitted to SCA via FTP

FTP credentials are stored in AWS SecretsManager

Both the entries file (.txt) and hash file (.hsh) must be received for the entry to be processed

Upon receipt, an email confirmation is sent to both compliance@dmsgroup.com and tkrausse@dmsgroup.com

Files are saved to S3 bucket sca-submissions for retention purposes
