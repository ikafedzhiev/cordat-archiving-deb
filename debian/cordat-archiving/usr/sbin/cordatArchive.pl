#!/usr/bin/perl

# Script to archive all cordat lots provided in a list from all sites specified in /etc/cordat.conf

use Getopt::Long;
use Storable qw(nstore retrieve);
use POSIX qw[tzset];
use Filesys::DiskSpace;
use DBD::Pg;
use DBI;
use DateTime;
use strict;

my ($sec, $min, $hour, $day, $month, $year)= localtime(time());
my $ArchivingDate= sprintf ("%4d%02d%02d",1900+$year,1+$month,$day);
my $start;
my $stop;
use Log::Log4perl qw(:easy);
Log::Log4perl::init('/etc/log4perl/log4cordat.conf');
my $log = Log::Log4perl->get_logger('archiving');
$log->info(" => Cardat Archiving is STARTING for date $ArchivingDate");
$ENV{'TZ'} = 'Europe/Brussels';
tzset();
my %Config;
&readConfigFile;
my $pgdb                =       $Config{PG_DB};
my $pghost              =       $Config{PG_HOST};
my $pgport              =       $Config{PG_PORT};
my $pguser              =       $Config{PG_USER};
my $pgpasswd            =       $Config{PG_PASSWD};
my $startdays         =       $Config{StartDate};
my $enddays           =       $Config{EndDate};

# Calculation start and stop date based on the values in the config file
$start = DateTime->new( year => 1900+$year, month => 1+$month, day => $day )->subtract(days => $startdays )->strftime('%Y%m%d');
$stop = DateTime->new( year => 1900+$year, month => 1+$month, day => $day )->subtract(days => $enddays )->strftime('%Y%m%d');

#Check for an ancient lockfile, and clean it up
system("find  /var/lock/cordat-archiving-process.lock -ctime +30 -delete");
unless (-e '/var/lock/cordat-archiving-process.lock') {
	system("touch /var/lock/cordat-archiving-process.lock");
} 

$log->info("$ArchivingDate : Cordat Archiving is STARTING for lots shipped between: $start and $stop");

my $pgdestdbh               = DBI->connect("dbi:Pg:dbname=$pgdb;host=$pghost;port=$pgport;","$pguser", "$pgpasswd")  
                    || {print STDERR "Cannot connect to archive DB\n" and $log->error("Cannot connect to archive DB") and exit 1};

my $sites                       = $Config{SitesDetails};
my @SitesDetails = split(/,/, $sites);

#temp files that will be used to store lots and validation data and which will be cleaned up after
my $lotfile             =       "/tmp/lot_list_".$ArchivingDate;
my $linuxtesterslist    =       "/tmp/linuxtesters_recent_".$ArchivingDate;
my $linuxtestersage     =       $Config{CrossCheckAge};
my @lotlist;

# Get lots from visual apps
my $status = system("/usr/sbin/GetShippedLots.py $start $stop > $lotfile");
if($status != 0) {
	$log->error("Error generation loglist; exiting.");
}

# Cleanup output
$status = system("sed -i 's/\r//g' $lotfile"); 
if($status != 0) { 
        $log->error("Error cleaning loglist; exiting."); 
} 

# Get recently updated lots from linuxtesters
$status = system("find /mnt/sofia/linuxtesters/testlog -mtime $linuxtestersage > $linuxtesterslist ; find /mnt/erfurt/linuxtesters/testlog -mtime $linuxtestersage >> $linuxtesterslist ; find /mnt/ieper/linuxtesters/testlog -mtime $linuxtestersage >> $linuxtesterslist");
if($status != 0) { 
        $log->error("Error generation list for validation against logfiles; exiting."); 
} 

# Do a crosscheck and put all lots for archiving in the lotlist
open (LIST, "$lotfile") or die "Can't open $lotfile for read: $!" and $log->error("Error: Cannot open lotlist");
open (VALIDATE, "$linuxtesterslist")  or die "Can't open $linuxtesterslist for read: $!" and $log->error("Error: Cannot open validation list");
while (<LIST>) {
	my $entry = $_;
	if (grep{/$entry/i} <VALIDATE>) {
		$log->info("Lot ".$entry." was modified recently and will not be archived");
	}
	else {
		push (@lotlist, $entry);	
	}
}
close LIST or die "Cannot close $lotfile: $!" and $log->error("Error: Cannot close lotlist");
close VALIDATE or die "Cannot close $linuxtesterslist: $!" and $log->error("Error: Cannot close validation list");
system ("rm -f $linuxtesterslist $lotfile");

# Process all valid lots
foreach my $lot(@lotlist) {
	foreach my $sd(@SitesDetails) {
		$lot = lc($lot);
		chomp($lot);	
		$sd =~ s/\s+//g; 
 		my ($site, $sourcehost ) = split(/:/, $sd);
		my $newtablename = '';
		my $tablename = '';

		$log->info(" => Checking $site for $lot");

                my $pgsourcedbh = DBI->connect("dbi:Pg:dbname=$pgdb;host=$sourcehost;port=$pgport;","$pguser", "$pgpasswd") 
                    || {print STDERR "Cannot connect to source DB\n" and $log->error("Cannot connect to source DB") and exit 1};

#Check if the lot exists
		my $query = "select tablename from pg_tables where schemaname='public' and tablename like 'dev%_lot".$lot."'";

		my $pgsth = $pgsourcedbh->prepare("$query");
		my $rv = $pgsth->execute();

		if (!defined($rv)) {
			$log->error("ERROR: ".$DBI::errstr);
 		}

		my $hash_ref;
		while ($hash_ref = $pgsth->fetchrow_hashref) {
                	my %record = %$hash_ref if $hash_ref;
			$log->info("Lot found, archiving: ".$record{tablename});
			$tablename = $record{tablename};
			$newtablename = $tablename.'_'.$site.'_'.$ArchivingDate;
			
			system ("export PGPASSWORD=".$pgpasswd."; pg_dump -i -h ".$sourcehost." -U ".$pguser." -t ".$tablename." ".$pgdb."  | sed -e 's/".$tablename."/".$newtablename."/g' | psql -h ".$pghost." -U ".$pguser." ".$pgdb);
			$log->info("Lot copying done! Verifying transfer");
			my $query_sr = "select count(*) as cnt from ".$tablename;
			my $pgsourcerows =  $pgsourcedbh->prepare("$query_sr");
	                my $rv = $pgsourcerows->execute();

        	        if (!defined($rv)) {
                	        $log->error("ERROR: ".$DBI::errstr);
	                }
                        my $query_dr = "select count(*) as cnt from ".$newtablename; 
                        my $pgdestrows =  $pgdestdbh->prepare("$query_dr"); 
                        my $rv = $pgdestrows->execute(); 
 
                        if (!defined($rv)) { 
                                $log->error("ERROR: ".$DBI::errstr); 
                        } 
			my $srows = $pgsourcerows->fetchrow_hashref;
			my $drows = $pgdestrows->fetchrow_hashref;
			if($srows->{cnt} == $drows->{cnt}) {
				$log->info("Verification Successful: transfer of ".$drows->{cnt}." complete. Deleting source record");
				my $query_drop = "drop table ".$tablename;
				my $pgdropsource = $pgsourcedbh->prepare("$query_drop");
				my $rv = $pgdropsource->execute();
		
				if (!defined($rv)) {
					$log->error("ERROR: ".$DBI::errstr);
				 }
				$pgdropsource->finish;
				$log->info("Dropped: ".$tablename." from: ".$sourcehost);
			}
			else {
				$log->info("Verification Failed: Source rows: ".$srows->{cnt}." while archived rows: ".$drows->{cnt}.". Keeping source record, cleaning up failed record.");
                                my $query_drop = "drop table ".$newtablename;
                                my $pgdropdest = $pgdestdbh->prepare("$query_drop");
                                my $rv = $pgdropdest->execute();
                                if (!defined($rv)) { 
                                        $log->error("ERROR: ".$DBI::errstr);
                                 } 
				$pgdropdest->finish;
                                $log->info("Dropped: ".$newtablename." from: ".$pghost);
			}
			$pgdestrows->finish;
			$pgsourcerows->finish;
		}
		$pgsth->finish;
		$pgsourcedbh->disconnect;
	}
}
$pgdestdbh->disconnect;
system("rm -f /var/lock/cordat-archiving-process.lock");

############################################
## Configuration setup
sub readConfigFile {

        open my $config, '<', '/etc/cordat.conf' or die $!;
        while(<$config>) {
            if ($_=~m/=/) 
              {   
                chomp;
                s/#.*//; 
                s/^\s+//;
                s/\s+$//;
                next unless length; 
                my ($key, $value) = split(/\s*=\s*/, $_,2);
                $Config{$key} = $value;
              } 
        }
}

