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
use Log::Log4perl qw(:easy);
Log::Log4perl::init('/etc/log4perl/log4cordat.conf');
my $log = Log::Log4perl->get_logger('archiving');
$ENV{'TZ'} = 'Europe/Brussels';
tzset();
my %Config;
&readConfigFile;
my $pgdb                =       $Config{PG_DB};
my $pghost              =       $Config{PG_HOST};
my $pgport              =       $Config{PG_PORT};
my $pguser              =       $Config{PG_USER};
my $pgpasswd            =       $Config{PG_PASSWD};


if($#ARGV !=2 ){
	$log->error("Number of parameters is wrong. Aborting..");
	print("Usage: cordatArchive.pl <site> <lot>");
        exit 1;
}
my $argSite = $ARGV[0];
my $argLot = $ARGV[1];

$log->info("$ArchivingDate : Cordat Archiving is STARTING for lot $argLot on $argSite .");


my $pgdestdbh               = DBI->connect("dbi:Pg:dbname=$pgdb;host=$pghost;port=$pgport;","$pguser", "$pgpasswd")  
                    || {print STDERR "Cannot connect to archive DB\n" and $log->error("Cannot connect to archive DB") and exit 1};

my $sites                       = $Config{SitesDetails};
my @SitesDetails = split(/,/, $sites);


foreach my $sd(@SitesDetails) {
	my $lot = lc($argLot);
	chomp($lot);	
	$sd =~ s/\s+//g; 
	my ($site, $sourcehost ) = split(/:/, $sd);

	if( lc($site) eq lc($argSite)) {
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
			exit 1;
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
				exit 1;
	                }
                        my $query_dr = "select count(*) as cnt from ".$newtablename; 
                        my $pgdestrows =  $pgdestdbh->prepare("$query_dr"); 
                        my $rv = $pgdestrows->execute(); 
 
                        if (!defined($rv)) { 
                                $log->error("ERROR: ".$DBI::errstr); 
				exit 1;
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
					exit 1;
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
					exit 1;
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
