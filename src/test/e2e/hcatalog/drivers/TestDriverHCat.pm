package TestDriverHCat;

###################
# 
# In addition to what TestDriverPig.pm does, this should also support:
#
# * Hive
# * HCat CLI 
# * Hadoop commands with result to stdout (e.g. 'fs')
# * Hadoop mapreduce jobs (i.e. result in files)
#
# The two latter ones are implemented like the 'pig' directive
# in that just 'hadoop' is used, and if the verification directive is 'sql'
# then a benchmark file will be compared with the output file,
# othewise stdout or stderr plus rc can be checked against verification directives. 
# 
# Based upon the consolidated Pig driver, which
# supports what privously was handled by:
#
# - TestDriverPig.pm
# - TestDriverScript.pm
# - TestDriverPigCmdLine.pm
# - TestDriverPigMultiQuery.pm
#
# Some code are ripe to be factored out. In interest of time, not done now...


# THINGS STIL TO DEAL WITH MARKED AS:

##!!!

# NOTE in particular that postProcessSingleOutputFile might need to be added to some run subs. 

###############################################################################

#use Miners::Test::TestDriver;
use TestDriver;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method
use Digest::MD5 qw(md5_hex);
use Util;
use File::Path;

use strict;
use English;

our $className= "TestDriver";
our @ISA = "$className";
our $ROOT = (defined $ENV{'HARNESS_ROOT'} ? $ENV{'HARNESS_ROOT'} : die "ERROR: You must set environment variable HARNESS_ROOT\n");
our $toolpath = "$ROOT/libexec/PigTest";

my $passedStr  = 'passed';
my $failedStr  = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr  = 'failed_dependency';

sub new
{
    # Call our parent
    my ($proto) = @_;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new;

    $self->{'exectype'} = "mapred"; # till we know better (in globalSetup())!
    $self->{'ignore'} = "true";     # till we know better (in globalSetup())!

    bless($self, $class);
    return $self;
}

###############################################################################
# This method has been copied over from TestDriver to make changes to
# support skipping tests which do not match current execution mode
# or which were marked as 'ignore'
#
#
# Static function, can be used by test_harness.pl
# Print the results so far, given the testStatuses hash.
# @param testStatuses - reference to hash of test status results.
# @param log - reference to file handle to print results to.
# @param prefix - A title to prefix to the results
# @returns nothing.
#
sub printResults
{
    my ($testStatuses, $log, $prefix) = @_;

    my ($pass, $fail, $abort, $depend, $skipped) = (0, 0, 0, 0, 0);

    foreach (keys(%$testStatuses)) {
        ($testStatuses->{$_} eq $passedStr)  && $pass++;
        ($testStatuses->{$_} eq $failedStr)  && $fail++;
        ($testStatuses->{$_} eq $abortedStr) && $abort++;
        ($testStatuses->{$_} eq $dependStr)  && $depend++;
        ($testStatuses->{$_} eq $skippedStr) && $skipped++;
    }   

    my $total = $pass + $fail + $skipped + $abort + $depend;

    my $msg = "$prefix, PASSED: $pass FAILED: $fail SKIPPED: $skipped ABORTED: $abort " .
              "FAILED DEPENDENCY: $depend TOTAL: $total";
    print $log "$msg\n";
    print "$msg\r";
}


sub replaceParameters
{
##!!! Move this to Util.pm

    my ($self, $cmd, $outfile, $testCmd, $log) = @_;

    # $self
    $cmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    # $outfile
    $cmd =~ s/:OUTPATH:/$outfile/g;

    # The same directory where .pig, .sh, .hcat, out/ are produced for the run:
    $cmd =~ s/:RUNDIR:/$testCmd->{'localpath'}/g;

    # $ENV
    $cmd =~ s/:PIGHARNESS:/$ENV{HARNESS_ROOT}/g;

    # $testCmd
    $cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
    $cmd =~ s/:OUTPATH:/$outfile/g;
    $cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
    $cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
    $cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
    $cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
    $cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
    $cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
    $cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
    $cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
    $cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
    $cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
    $cmd =~ s/:ZEBRAJAR:/$testCmd->{'zebrajar'}/g;
    $cmd =~ s/:FILER:/$testCmd->{'filerPath'}/g;
    $cmd =~ s/:GRIDSTACK:/$testCmd->{'gridstack.root'}/g;
    $cmd =~ s/:USER:/$ENV{USER}/g;

    if ( $testCmd->{'hadoopSecurity'} eq "secure" ) { 
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteSecureCluster'}/g;
    } else {
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteNotSecureCluster'}/g;
    }

    # extra for hive, hcat, hadoop cmd
    $cmd =~ s/:THRIFTSERVER:/$testCmd->{'thriftserver'}/g;
    $cmd =~ s/:HADOOP_CLASSPATH:/$testCmd->{'hadoop_classpath'}/g;
    $cmd =~ s/:HCAT_JAR:/$testCmd->{'hcatalog.jar'}/g;

    # used in script call to `java :CLASSPATH: ...` in bootstrap_hcat.conf
    $cmd =~ s/:CLASSPATH:/$testCmd->{'classpath'}/g;

    return $cmd;
}

sub hiveWorkArounds
{
    my ($self, $cmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # return $cmd;

    # Work-around for Hive problem where INSERT OVERWRITE failed when called w/o hive.merge.mapfiles=false
    if ($cmd =~ /insert overwrite/i) {
      print $log "$0:$subName WARNING: setting hive.merge.mapfiles in command\n";
      $cmd = "\nset hive.merge.mapfiles=false;\n$cmd";
 #  } else {
 #    print $log "$0:$subName DEBUG: NOT setting hive.merge.mapfiles in hive command\n";
    }

    return $cmd;
}



sub globalSetup
{
    my ($self, $globalHash, $log) = @_;
    my $subName = (caller(0))[3];


    # Setup the output path
    my $me = `whoami`;
    chomp $me;
    $globalHash->{'runid'} = $me . "." . time;

    # if "-ignore false" was provided on the command line,
    # it means do run tests even when marked as 'ignore'
    if(defined($globalHash->{'ignore'}) && $globalHash->{'ignore'} eq 'false')
    {
        $self->{'ignore'} = 'false';
    }

    # if "-x local" was provided on the command line,
    # it implies pig should be run in "local" mode -so 
    # change input and output paths 
    if(defined($globalHash->{'x'}) && $globalHash->{'x'} eq 'local')
    {
        $self->{'exectype'} = "local";
        $globalHash->{'inpathbase'} = $globalHash->{'localinpathbase'};
        $globalHash->{'outpathbase'} = $globalHash->{'localoutpathbase'};
    }
    $globalHash->{'outpath'} = $globalHash->{'outpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";

    # extract the current zebra.jar file path from the classpath
    # and enter it in the hash for use in the substitution of :ZEBRAJAR:
    my $zebrajar = $globalHash->{'cp'};
    $zebrajar =~ s/zebra.jar.*/zebra.jar/;
    $zebrajar =~ s/.*://;
    $globalHash->{'zebrajar'} = $zebrajar;

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {
        $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
    }
    else {
        $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }

    my $tmpUsePig = $globalHash->{'use-pig.pl'};
    $globalHash->{'use-pig.pl'} = 1;
    my @cmd = (Util::getBasePigCmd($globalHash), '-e', 'mkdir', $globalHash->{'outpath'});
    $globalHash->{'use-pig.pl'} = $tmpUsePig;

    if($self->{'exectype'} eq "local")
    {
        @cmd = ('mkdir', '-p', $globalHash->{'outpath'});
    }


    if($self->{'exectype'} eq "mapred")
    {
       my $id = `id -un`;
       chomp $id;
       if ($id eq 'root') {
         # my @suCmd = ('su', 'hadoopqa', '-c', "'" . join(' ', @cmd) . "'");
         # print $log join(" ", @suCmd) . "\n";
         # IPC::Run::run(\@suCmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
	 # above failed, doing below for now...

         my $command= join (" ", @cmd);
         $command = "echo \"$command\" | su hadoopqa 2>&1";
         print $log "$command\n";
         my @result=`$command`;
         my $rc = $? >> 8;
         print $log "Output from create HDFS directory: " . join (" ", @result) . "\n";
         die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n" if $rc != 0;

       } else {
         print $log join(" ", @cmd) . "\n";
         IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
       }
    }
    else
    {
       IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create directory " . $globalHash->{'outpath'} . "\n";
    }

    IPC::Run::run(['mkdir', '-p', $globalHash->{'localpath'}], \undef, $log, $log) or 
        die "Cannot create localpath directory " . $globalHash->{'localpath'} .
        " " . "$ERRNO\n";
}

sub getCommand
{
    my ($self, $testCmd ) = @_;

    if( $testCmd->{'pig'} ){
       return "pig";
    } elsif( $testCmd->{'hadoop'} ){
       return "hadoop";
    } elsif( $testCmd->{'hive'} ){
       return "hive";
    } elsif( $testCmd->{'hcat'} ){
       return "hcat";
    } elsif(  $testCmd->{'script'} ){
       return "script";
    } else {
       return "";
    }
}

sub runTest
{
    my ($self, $testCmd, $log, $copyResults) = @_;
    my $subName  = (caller(0))[3];

    # check is root if using 'run_as'
    if (defined($testCmd->{'run_as'}) && $testCmd->{'run_as'} ne '') {
       my $id = `id -un`;
       chomp $id;
       if ($id ne 'root') {
         die "$subName FATAL You have to run as root to use the 'run_as' directive, you are: $id";
       }
    }

    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'pig'} && $self->hasCommandLineVerifications( $testCmd, $log) ) {
       return $self->runPigCmdLine( $testCmd, $log, $copyResults );
    } elsif( $testCmd->{'pig'} ){
       return $self->runPig( $testCmd, $log, $copyResults );
    } elsif ( $testCmd->{'hadoop'} && $self->hasCommandLineVerifications( $testCmd, $log) ) {
       return $self->runHadoopCmdLine( $testCmd, $log, $copyResults );
    } elsif( $testCmd->{'hadoop'} ){
       return $self->runHadoop( $testCmd, $log, $copyResults );
    } elsif( $testCmd->{'hive'} ){
       return $self->runHive( $testCmd, $log, $copyResults );
    } elsif( $testCmd->{'hcat'} ){
       return $self->runHCat( $testCmd, $log, $copyResults );
    } elsif(  $testCmd->{'script'} ){
       return $self->runScript( $testCmd, $log );
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}


sub runPigCmdLine
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the pig script to a file.
    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);

    # Build the command
    my @baseCmd = Util::getBasePigCmd($testCmd);
    my @cmd = @baseCmd;

    # Add option -l giving location for secondary logs
    ##!!! Should that even be here? 
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-logfile");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    # Add pig file and redirections 
    push(@cmd, $pigfile);
    my $command= join (" ", @cmd);
    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      $command = 'echo "' . $command . '"' . " | su $testCmd->{'run_as'}";
    }
    $command= "$command 1> $stdoutfile 2> $stderrfile";


    # Run the command
    print $log "$0:$subName Going to run command: $command\n";
    print $log "$0:$subName STD OUT IS IN FILE: $stdoutfile\n";
    print $log "$0:$subName STD ERROR IS IN FILE: $stderrfile\n";
    print $log "$0:$subName PIG SCRIPT FILE, $pigfile, CONTAINS:\n<$pigcmd>\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    # Here and other run* should do:
    # If expected rc defined and = 0 and actual rc <> 0 then
    # die "Failed running $pigfile\n";

    print $log "STD ERROR CONTAINS:\n<$result{'stderr'}>\n";

    return \%result;
}

sub runHive
# The code is based on the run runHadoopCmdLine,
# but with the difference that it's output from stdout
# can be used for both comparions against benchmark file and
# verification by pattern matching depending on wether the test
# has a 'sql' or a pattern match directive.
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $hivefile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".hive";
    # my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    my $outfile = $stdoutfile; # For Hive, there is only the stdout, and that is being captured by 
    # the way the command is run. So, the hive command should _not_ use :OUTFILE: 

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e $outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the hive command to a file.
    my $hivecmd = $self->replaceParameters( $testCmd->{'hive'}, $outfile, $testCmd, $log );

    $hivecmd = $self->hiveWorkArounds( $hivecmd, $log );


    open(FH, "> $hivefile") or die "Unable to open file $hivefile to write hive command, $ERRNO\n";
    print FH "$hivecmd\n";
    close(FH);

    # Build the command
    my @cmd = Util::getHiveCmd($testCmd);

    #Add metastore info
    push(@cmd, "--hiveconf hive.metastore.local=false --hiveconf hive.metastore.uris=thrift://".$testCmd->{'thriftserver'});
   
   
    if( defined($testCmd->{'metastore.principal'}) && ($testCmd->{'metastore.principal'} =~ m/\S+/) 
        &&  ($testCmd->{'metastore.principal'} ne '${metastore.principal}')){
        push(@cmd, "--hiveconf hive.metastore.sasl.enabled=true  --hiveconf hive.metastore.kerberos.principal=$testCmd->{'metastore.principal'}");
    } else {
        push(@cmd, "--hiveconf hive.metastore.sasl.enabled=false");
    }

    # Add hive command file
    push(@cmd, '-f', $hivefile); 

    # Add redirections 
    # no need to split, as not using IPC run.
    my $command= join (" ", @cmd);
     # Add hive command line arguments if they're provided
    if (defined($testCmd->{'hive_cmdline_args'})) {
    $command = $command . $testCmd->{'hive_cmdline_args'}
    }

   # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      $command = "echo \"$command\" | su $testCmd->{'run_as'}";
    }
    $command= "$command 1> $stdoutfile 2> $stderrfile";


    # Run the command
    print $log "$0:$subName Going to run command: $command\n";
    print $log "$0:$subName STD OUT IS IN FILE: $stdoutfile\n";
    print $log "$0:$subName STD ERROR IS IN FILE: $stderrfile\n";
    print $log "$0:$subName HIVE QUERY FILE, $hivefile, CONTAINS:\n<$hivecmd>\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`; # This could be big. Left for now, as compareScript relies on it 
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n<$result{'stderr'}>\n";

    $result{'output'} = $self->postProcessSingleOutputFile($outfile, $outdir, undef, $testCmd, $log);
    $result{'originalOutput'} = "$outdir/out_original"; # populated by postProcessSingleOutputFile

    return \%result;
} # end sub runHive


sub runHCat
# COPY of runHive for now
# When HCat CLI is implemented, then change!!!
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $hcatfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".hcat";
    # my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    my $outfile = $stdoutfile; # For HCat, there is only the stdout, and that is being captured by 
    # the way the command is run. So, the hcat command should _not_ use :OUTFILE: 

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e $outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the hcat command to a file.
    my $hcatcmd = $self->replaceParameters( $testCmd->{'hcat'}, $outfile, $testCmd, $log );
    # $hcatcmd = "set hive.metastore.uris=thrift://gwbl2004.blue.ygrid.yahoo.com:9080;\n$hcatcmd";

    $hcatcmd = $self->hiveWorkArounds( $hcatcmd, $log );

    open(FH, "> $hcatfile") or die "Unable to open file $hcatfile to write hcat command, $ERRNO\n";
    print FH $hcatcmd . "\n";
    close(FH);

    # Build the command
    my @cmd = Util::getHCatCmd($testCmd);

    # Add hcat command line arguments if they're provided
    if (defined($testCmd->{'hcat_cmdline_args'})) {
        push(@cmd, @{$testCmd->{'hcat_cmdline_args'}});
    }

    # Add hcat command file
    if (defined($testCmd->{'hcat_cmdline_use_-e_switch'})) {
      if (defined($testCmd->{'run_as'})) {
        push(@cmd, '-e', '\"' . $hcatcmd . '\"');
      } else {
        push(@cmd, '-e', '"' . $hcatcmd . '"');
      }
    } else {
      push(@cmd, '-f', $hcatfile); 
    }

    # Add redirections 
    # no need to split, as not using IPC run.
    my $command= join (" ", @cmd);
    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      $command = "echo \"$command\" | su $testCmd->{'run_as'}";
    }
    $command= "$command 1> $stdoutfile 2> $stderrfile";

    # Run the command
    print $log "$0:$subName Going to run command: $command\n";
    print $log "$0:$subName STD OUT IS IN FILE: $stdoutfile\n";
    print $log "$0:$subName STD ERROR IS IN FILE: $stderrfile\n";
    print $log "$0:$subName HCAT QUERY FILE, $hcatfile, CONTAINS:\n<$hcatcmd>\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`; # This could be big. Left for now, as compareScript relies on it 
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n<$result{'stderr'}>\n";

    $result{'output'} = $self->postProcessSingleOutputFile($outfile, $outdir, undef, $testCmd, $log);
    $result{'originalOutput'} = "$outdir/out_original"; # populated by postProcessSingleOutputFile

    return \%result;
} # end sub runHCat


sub runHadoopCmdLine
# Modified from runPigCmdLine
# !!! Works, but need to add other arguments, like queue...???
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $hadoopfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".hadoop";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the hadoop command to a file.
    my $hadoopcmd = $self->replaceParameters( $testCmd->{'hadoop'}, $outfile, $testCmd, $log );

    # adjust for the leading and trailing new line often seen in the conf file's command directives
    $hadoopcmd =~ s/^\s*(.*?)\s*$/\1/s;

    open(FH, "> $hadoopfile") or die "Unable to open file $hadoopfile to write hadoop command, $ERRNO\n";
    print FH $hadoopcmd . "\n";
    close(FH);

    # Build the command
    my @baseCmd = Util::getHadoopCmd($testCmd);
    my @cmd = @baseCmd;

    # Add command line arguments if they're provided
    if (defined($testCmd->{'hadoop_cmdline_args'})) {
        push(@cmd, @{$testCmd->{'hadoop_cmdline_args'}});
    }

    # Add hadoop command and redirections 
    push(@cmd, $hadoopcmd); # no need to split, as not using IPC run.
    my $command= join (" ", @cmd);
    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      $command = "echo \"$command\" | su $testCmd->{'run_as'}";
    }
    $command= "$command 1> $stdoutfile 2> $stderrfile";

    #Set HADOOP_CLASSPATH environment variable if provided
    if (defined($testCmd->{'hadoop_classpath'})) {
        my $hadoop_classpath = $self->replaceParameters( $testCmd->{'hadoop_classpath'}, $outfile, $testCmd, $log );
        $ENV{'HADOOP_CLASSPATH'} = $ENV{'HCAT_EXTRA_JARS'};
    }
    my $hadoop_opts = "-Dhive.metastore.uris=thrift://".$testCmd->{'thriftserver'}." -Dhcat.metastore.uri=thrift://".$testCmd->{'thriftserver'};
    if( defined($testCmd->{'metastore.principal'}) && ($testCmd->{'metastore.principal'} =~ m/\S+/)
        &&  ($testCmd->{'metastore.principal'} ne '${metastore.principal}')){
	$hadoop_opts = join '',$hadoop_opts," -Dhive.metastore.sasl.enabled=true -Dhcat.metastore.principal=",
                            $testCmd->{'metastore.principal'}," -Dhive.metastore.kerberos.principal=",$testCmd->{'metastore.principal'};
    } else {
        $hadoop_opts = join '',$hadoop_opts," -Dhive.metastore.sasl.enabled=false";
    }
    $ENV{'HADOOP_OPTS'} = $hadoop_opts;
    # Run the command
    print $log "$0:$subName Going to run command: $command\n";
    print $log "$0:$subName STD OUT IS IN FILE: $stdoutfile\n";
    print $log "$0:$subName STD ERROR IS IN FILE: $stderrfile\n";
    print $log "$0:$subName HADOOP COMMAND FILE, $hadoopfile, CONTAINS:\n<$hadoopcmd>\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    # $result{'output'} = $outfile;
    $result{'output'} = $stdoutfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n<$result{'stderr'}>\n";

  #!!!!!!!!!!!!!! IS this be needed here????
  # my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
  # $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
  # $result{'originalOutput'} = "$outdir/out_original"; # populated by postProcessSingleOutputFile

    return \%result;
} # end sub runHadoopCmdLine


sub runScript
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $script = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".sh";
    my $outdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outfile = "$outdir/script.out";
    my $stdoutfile = "$outdir/script.out";
    my $stderrfile = "$outdir/script.err";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the script to a file
    my $cmd = $self->replaceParameters( $testCmd->{'script'}, $outfile, $testCmd, $log );

    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH $cmd . "\n";
    close(FH);

    my @result=`chmod +x $script`;

    # Build the command
    my $command;
    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      $command = "cat $script | su $testCmd->{'run_as'}";
    } else {
      $command= "$script";
    }
    $command= "$command 1> $stdoutfile 2> $stderrfile";

    # Run the script
    print $log "$0:$subName Going to run command: $command\n";
    print $log "$0:$subName STD OUT IS IN FILE ($stdoutfile)\n";
    print $log "$0:$subName STD ERROR IS IN FILE ($stderrfile)\n";
    print $log "$0:$subName SCRIPT IS IN FILE ($script)\n";
    print $log "$0:$subName SCRIPT CONTAINS:\n<$cmd>\n";

    @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n<$result{'stderr'}>\n";

    return \%result;
}


sub runHadoop
# Being modified from runPig
# !!! Works, but need to add other arguments, like queue...???
{
    my ($self, $testCmd, $log, $copyResults) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the hadoop command to a file.
    my $hadoopfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".hadoop";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $hadoopcmd = $self->replaceParameters( $testCmd->{'hadoop'}, $outfile, $testCmd, $log );

    # adjust for the leading and trailing new line often seen in the conf file's command directives
    $hadoopcmd =~ s/^\s*(.*?)\s*$/\1/s;

    open(FH, "> $hadoopfile") or die "Unable to open file $hadoopfile to write hadoop command file, $ERRNO\n";
    print FH $hadoopcmd . "\n";
    close(FH);


    # Build the command
    my @cmd = Util::getHadoopCmd($testCmd);

    # Add command line arguments if they're provided
    if (defined($testCmd->{'hadoop_cmdline_args'})) {
        push(@cmd, @{$testCmd->{'hadoop_cmdline_args'}});
    }

    # Add the test command elements
    push(@cmd, split(/ +/,$hadoopcmd));

    # Set HADOOP_CLASSPATH environment variable if provided
    if (defined($testCmd->{'hadoop_classpath'})) {
        my $hadoop_classpath = $self->replaceParameters( $testCmd->{'hadoop_classpath'}, $outfile, $testCmd, $log );
        $ENV{'HADOOP_CLASSPATH'} = $ENV{'HCAT_EXTRA_JARS'};
    }

    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      my $cmd = '"' . join (" ", @cmd) . '"';
      @cmd = ("echo", $cmd, "|", "su", $testCmd->{'run_as'});
    }

    my $script = $hadoopfile . ".sh";
    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH join (" ", @cmd) . "\n";
    close(FH);
    my @result=`chmod +x $script`;

    # Run the command
    print $log "$0::$className::$subName INFO: Going to run hadoop command in shell script: $script\n";
    print $log "$0::$className::$subName INFO: Going to run hadoop command: " . join(" ", @cmd) . "\n";

    my @runpig = ("$script");
    IPC::Run::run(\@runpig, \undef, $log, $log) or
        die "Failed running $script\n";
    $result{'rc'} = $? >> 8;

    # Get results from the command locally
    my @basePigCmd = Util::getBasePigCmd($testCmd);

    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my @SQLQuery = @{$testCmd->{'queries'}}; # here only used to determine if single-guery of multi-query


    # mapreduce
    if($self->{'exectype'} eq "mapred")
    {
        # single query
        if ($#SQLQuery == 0) {
            if ($copyResults) {
              $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@basePigCmd, $testCmd, $log);
              $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
            } else {
              $result{'output'} = "NO_COPY";
            }
        }
    }
    # local mode
    else 
    {
        # single query
        if ($#SQLQuery == 0) {
            $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".dir";
            mkdir $localdir;
            $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@basePigCmd, $testCmd, $log);
            $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
        } 
    }

    # Compare doesn't get the testCmd hash, so I need to stuff the necessary
    # info about sorting into the result.
    if (defined $testCmd->{'sortArgs'} && $testCmd->{'sortArgs'}) {
        $result{'sortArgs'} = $testCmd->{'sortArgs'};
    }

    return \%result;
} # end sub runHadoop



sub runPig
{
    my ($self, $testCmd, $log, $copyResults) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);


    # Build the command
    my @baseCmd = Util::getBasePigCmd($testCmd);
    my @cmd = @baseCmd;

    # Add option -l giving location for secondary logs
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-logfile");
    push(@cmd, $locallog);
    
    my $pig_opts = "-Dhive.metastore.uris=thrift://".$testCmd->{'thriftserver'}." -Dhcat.metastore.uri=thrift://".$testCmd->{'thriftserver'};
    if( defined($testCmd->{'metastore.principal'}) && ($testCmd->{'metastore.principal'} =~ m/\S+/)
         &&  ($testCmd->{'metastore.principal'} ne '${metastore.principal}')){
	$pig_opts = join '',$pig_opts," -Dhive.metastore.sasl.enabled=true -Dhcat.metastore.principal=",
                         $testCmd->{'metastore.principal'}," -Dhive.metastore.kerberos.principal=",$testCmd->{'metastore.principal'};
    } else {
        $pig_opts = join '',$pig_opts," -Dhive.metastore.sasl.enabled=false";
    }    
    $ENV{'PIG_OPTS'} = $pig_opts;
  
    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    push(@cmd, $pigfile);

    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      my $cmd = '"' . join (" ", @cmd) . '"';
      @cmd = ("echo", $cmd, "|", "su", $testCmd->{'run_as'});
    }

    my $script = $pigfile . ".sh";
    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH join (" ", @cmd) . "\n";
    close(FH);
    my @result=`chmod +x $script`;

    # Run the command
    print $log "$0::$className::$subName INFO: Going to run pig command in shell script: $script\n";
    print $log "$0::$className::$subName INFO: Going to run pig command: " . join(" ", @cmd) . "\n";

    my @runpig = ("$script");
    IPC::Run::run(\@runpig, \undef, $log, $log) or
        die "Failed running $script\n";
    $result{'rc'} = $? >> 8;

    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my @SQLQuery = @{$testCmd->{'queries'}}; # here only used to determine if single-guery of multi-query
       
    # mapreduce
    if($self->{'exectype'} eq "mapred")
    {
        # single query
        if ($#SQLQuery == 0) {
            if ($copyResults) {
              $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
              $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
            } else {
              $result{'output'} = "NO_COPY";
            }
        }
        # multi query
        else {
            my @outfiles = ();
            for (my $id = 1; $id <= ($#SQLQuery + 1); $id++) {
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                $localoutfile = $outfile . ".$id";

                # Copy result file out of hadoop
                my $testOut;
                if ($copyResults) {
                  $testOut = $self->postProcessSingleOutputFile($localoutfile, $localdir, \@baseCmd, $testCmd, $log);
                } else {
                  $testOut = "NO_COPY";
                }
                push(@outfiles, $testOut);
            }
            ##!!! originalOutputs not set! Needed?
            $result{'outputs'} = \@outfiles;
        }
    }
    # local mode
    else 
    {
        # single query
        if ($#SQLQuery == 0) {
            $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".dir";
            mkdir $localdir;
            $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
            $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
        } 
        # multi query
        else {
            my @outfiles = ();
            for (my $id = 1; $id <= ($#SQLQuery + 1); $id++) {
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
                mkdir $localdir;
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                mkdir $localdir;
                $localoutfile = $outfile . ".$id";

                my $testRes = $self->postProcessSingleOutputFile($localoutfile, $localdir, \@baseCmd, $testCmd, $log);
                push(@outfiles, $testRes);
            }
            ##!!! originalOutputs not set!
            $result{'outputs'} = \@outfiles;
        }
    }

    # Compare doesn't get the testCmd hash, so I need to stuff the necessary
    # info about sorting into the result.
    if (defined $testCmd->{'sortArgs'} && $testCmd->{'sortArgs'}) {
        $result{'sortArgs'} = $testCmd->{'sortArgs'};
    }

    return \%result;
}


sub postProcessSingleOutputFile
{
    my ($self, $outfile, $localdir, $baseCmd, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    my $from_hdfs;
    if ( ($testCmd->{'pig'} || $testCmd->{'hadoop'}) && ($self->{'exectype'} eq "mapred")) {
      $from_hdfs = 1;
    } else {
      $from_hdfs = 0;
    }

    # Copy to local if results on HDFS
    if ( $from_hdfs  ) {
        my @baseCmd = @{$baseCmd};
        my @copyCmd = @baseCmd;
        push(@copyCmd, ('-e', 'copyToLocal', $outfile, $localdir)); 
        print $log "$0::$className::$subName INFO: Going to run pig command: " . join(" ", @copyCmd) . "\n";
 

       my $id = `id -un`;
       chomp $id;
       if ($id eq 'root') {
         # my @suCmd = ('su', 'hadoopqa', '-c', "'" . join(' ', @cmd) . "'");
         # print $log join(" ", @suCmd) . "\n";
         # IPC::Run::run(\@suCmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
	 # above failed, doing below for now...

         my $command= join (" ", @copyCmd);
         $command = "echo \"$command\" | su hadoopqa 2>&1";
         print $log "$command\n";
         my @result=`$command`;
         my $rc = $? >> 8;
         print $log "Output from copy from HDFS: " . join (" ", @result) . "\n";
         die "Cannot copy results from HDFS $outfile to $localdir\n" if $rc != 0;

       } else {
         print $log join(" ", @copyCmd) . "\n";
         IPC::Run::run(\@copyCmd, \undef, $log, $log) or die "Cannot copy results from HDFS $outfile to $localdir\n";
       }

    }


    # Sort the result if necessary.  Keep the original output in one large file.
    # Use system not IPC run so that the '*' gets interpolated by the shell.
    
    # Build command to:
    # 1. Combine part files
    my $fppCmd = 
            ($from_hdfs  ) ? "cat $localdir/map* $localdir/part* 2>/dev/null" : 
            (-d $outfile ) ? "cat $outfile/part* 2>/dev/null" :
                             "cat $outfile";
    
    # 2. Standardize float precision
    if (defined $testCmd->{'floatpostprocess'} && defined $testCmd->{'delimiter'}) {
        $fppCmd .= " | $toolpath/floatpostprocessor '" . $testCmd->{'delimiter'} . "'";
    }
    
    $fppCmd .= " > $localdir/out_original";
    
    # run command
    print $log "$fppCmd\n";
    system($fppCmd);


    # Sort the results for the benchmark compare.
    if ( $testCmd->{'sortResults'} eq '1' ) {
      my @sortCmd = ('sort', "$localdir/out_original");
      print $log join(" ", @sortCmd) . "\n";
      IPC::Run::run(\@sortCmd, '>', "$localdir/out_sorted");
      return "$localdir/out_sorted";
   } else {
      return "$localdir/out_original";
   }
}

sub generateBenchmark
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    my @SQLQuery = @{$testCmd->{'queries'}};
 
    if ($#SQLQuery == 0) {
        my $outfile = $self->generateSingleSQLBenchmark($testCmd, $SQLQuery[0], undef, $log);
        $result{'output'} = $outfile;
    } else {
        my @outfiles = ();
        for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
            my $sql = $SQLQuery[$id];
            my $outfile = $self->generateSingleSQLBenchmark($testCmd, $sql, ($id+1), $log); 
            push(@outfiles, $outfile);
        }
        $result{'outputs'} = \@outfiles;
    }

    return \%result;
}

sub generateSingleSQLBenchmark
{
    my ($self, $testCmd, $sql, $id, $log) = @_;
    my $subName  = (caller(0))[3];

    my $command_directive;
    if( $testCmd->{'pig'} ){
      $command_directive = 'pig';
    } elsif( $testCmd->{'hadoop'} ){
      $command_directive = 'hadoop';
    } elsif( $testCmd->{'hive'} ){
      $command_directive = 'hive';
    } elsif( $testCmd->{'hcat'} ){
      $command_directive = 'hcat';
    } elsif( $testCmd->{'script'} ){
      $command_directive = 'script';
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }

    my $qmd5 = substr(md5_hex($testCmd->{$command_directive}), 0, 5);
    my $outfile = $testCmd->{'benchmarkPath'} . "/" . $testCmd->{'group'} . "_" . $testCmd->{'num'};
    $outfile .= defined($id) ? ".$id" . ".expected." . $qmd5 :  ".expected." . $qmd5;


    print $log "Getting benchmark file: $outfile\n";
    
    if (-e $outfile) {
        return $outfile;
    }

    my @cmd = ('psql', '-U', $testCmd->{'dbuser'}, '-d', $testCmd->{'dbdb'},
        '-c', $sql, '-t', '-A', '--pset', "fieldsep='	'", '-o', $outfile);


    # To facilitate generating the benchmarks manually on a different machine, if postgres db not configured 
    # cmdForFile is as cmd above just with quotes around the $sql
    # Added extension '.sh' becuase the script now also does sort and float postprocessing if applicable
    my @cmdForFile = ('psql', '-U', $testCmd->{'dbuser'}, '-d', $testCmd->{'dbdb'},
        '-c', '"'.$sql.'"', '-t', '-A', '--pset', "fieldsep='	'", '-o', $outfile);
    my $psqlfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".psql.sh";
    open(FH, "> $psqlfile") or die "Unable to open file $psqlfile to write psql script, $ERRNO\n";
    print FH join(" ", @cmdForFile) . "\n";


    # Prepare to sort and postprocess the result if necessary
    my $shellCmd = "cat $outfile";
    if (defined $testCmd->{'floatpostprocess'} && defined $testCmd->{'delimiter'}) {
        $shellCmd .= " | $toolpath/floatpostprocessor '" . $testCmd->{'delimiter'} . "' ";
    }
    
    if ( $testCmd->{'sortBenchmark'} eq '1' ) {
      $shellCmd .= " | sort";
      if (defined $testCmd->{'sortBenchmarkArgs'}) { # but the pig test conf files don't use that anyway...
          $shellCmd .= " " . join(" ", @{$testCmd->{'sortBenchmarkArgs'}});
      }
    }

    my $tmpfile = $outfile . ".tmp";
    $shellCmd .= " > $tmpfile";

    # Complete the writing to file
    print FH "$shellCmd\n";
    print FH "mv $tmpfile $outfile\n";
    close(FH);
    print $log "SQL command file: $psqlfile\n";

    # Run...
    print $log "Running SQL command [" . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or do {
        print $log "Sql command <" . $sql .
            " failed for >>$testCmd->{group}_$testCmd->{num}<<\n";
        unlink $outfile if ( -e $outfile  );
    
        die "Sql command failed for >>$testCmd->{group}_$testCmd->{num}<<\n";
    };
    # Use system not IPC run so that any '*' gets interpolated by the shell.
    print $log "$shellCmd\n";
    system($shellCmd);
    unlink $outfile;
    IPC::Run::run ['mv', $tmpfile, $outfile];

    return $outfile;
}

sub hasCommandLineVerifications
{
    my ($self, $testCmd, $log) = @_;

    foreach my $key ('rc', 'expected_out', 'expected_out_regex', 'expected_err', 'expected_err_regex', 
                     'not_expected_out', 'not_expected_out_regex', 'not_expected_err', 'not_expected_err_regex' ) {
      if (defined $testCmd->{$key}) {
         return 1;
      }
    }
    return 0;
}


sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];
    # Returns 0 (false) for failed test, non-zero (true) for passed test 

    # For now, if the test has 
    # - testCmd pig, and 'sql' for benchmark, then use compareToBenchmark
    # - any verification directives formerly used by CmdLine or Script drivers (rc, regex on out and err...)
    #   then use compareScript even if testCmd is "pig"
    # - testCmd script, then use compareScript
    # - testCmd pig, and none of the above, then use compareToBenchmark
    #
    # Later, should add ability to have same tests both verify with the 'script' directives, 
    # and do a benchmark compare, if it was a pig cmd. E.g. 'rc' could still be checked when 
    # doing the benchmark compare.

    if( defined $testCmd->{'sql'} ){
       return $self->compareToBenchmark ( $testResult, $benchmarkResult, $log, $testCmd);
    } elsif( $self->hasCommandLineVerifications( $testCmd, $log) ){
       return $self->compareScript ( $testResult, $log, $testCmd);
    } elsif( $testCmd->{'pig'} ){
             # maybe using a custom benchmark file, and has no 'sql' directive
       return $self->compareToBenchmark ( $testResult, $benchmarkResult, $log, $testCmd);
    } else {
       print $log "$0.$subName WARNING Did not find a comparison method. Use 'noverify' if this is intented.\n";
       return 0;
    } 
}


sub compareScript
{
    my ($self, $testResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    # IMPORTANT NOTES:
    #
    # If you are using a regex to compare stdout or stderr
    # and if the pattern that you are trying to match spans two line
    # explicitly use '\n' (without the single quotes) in the regex
    #
    # If any verification directives are added here 
    # do remember also to add them to the hasCommandLineVerifications subroutine.
    #
    # If the test conf file misspells the directive, you won't be told...
    # 

    my $result = 1;  # until proven wrong...

    # Return Code
    if (defined $testCmd->{'rc'}) {                                                                             
      print $log "$0::$subName INFO Checking return code " .
                 "against expected <$testCmd->{'rc'}>\n";
      if ( (! defined $testResult->{'rc'}) || ($testResult->{'rc'} != $testCmd->{'rc'})) {                                                         
        print $log "$0::$subName INFO Check failed: rc = <$testCmd->{'rc'}> expected, test returned rc = <$testResult->{'rc'}>\n";
        $result = 0;
      }
    }

    # ???? Will that ever be needed?
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    # Standard Out
    if (defined $testCmd->{'expected_out'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'expected_out'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stdout " .
              "as exact match against expected <$pattern>\n";
      if ($testResult->{'stdout'} ne $pattern) {
        print $log "$0::$subName INFO Check failed: exact match of <$pattern> expected in stdout: <$testResult->{'stdout'}>\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'not_expected_out'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stdout " .
              "as NOT exact match against expected <$pattern>\n";
      if ($testResult->{'stdout'} eq $pattern) {
        print $log "$0::$subName INFO Check failed: NON-match of <$pattern> expected to stdout: <$testResult->{'stdout'}>\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_out_regex'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'expected_out_regex'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stdout " .
              "for regular expression <$pattern>\n";
      # if ($testResult->{'stdout'} !~ $pattern) {
      if ($testResult->{'stdout'} !~ /$pattern/m) {
        print $log "$0::$subName INFO Check failed: regex match of <$pattern> expected in stdout: <$testResult->{'stdout'}>\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out_regex'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'not_expected_out_regex'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stdout " .
              "for NON-match of regular expression <$pattern>\n";
      # if ($testResult->{'stdout'} =~ $pattern) {
      if ($testResult->{'stdout'} =~ /$pattern/m) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$pattern> expected in stdout: <$testResult->{'output'}>\n";
        # prints HDFS location, should give local
        $result = 0;
      }
    } 

    # Standard Error
    if (defined $testCmd->{'expected_err'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'expected_err'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stderr " .
              "as exact match against expected <$pattern>\n";
      if ($testResult->{'stderr'} ne $pattern) {
        print $log "$0::$subName INFO Check failed: exact match of <$pattern> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'not_expected_err'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stderr " .
              "as NOT an exact match against expected <$pattern>\n";
      if ($testResult->{'stderr'} eq $pattern) {
        print $log "$0::$subName INFO Check failed: NON-match of <$pattern> expected to stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_err_regex'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'expected_err_regex'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stderr " .
              "for regular expression <$pattern>\n";
      # if ($testResult->{'stderr'} !~ $pattern) {
      if ($testResult->{'stderr'} !~ /$pattern/m) {
        print $log "$0::$subName INFO Check failed: regex match of <$pattern> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err_regex'}) {
      my $pattern = $self->replaceParameters( $testCmd->{'not_expected_err_regex'}, $outfile, $testCmd, $log );
      print $log "$0::$subName INFO Checking test stderr " .
              "for NON-match of regular expression <$pattern>\n";
      # if ($testResult->{'stderr'} =~ $pattern) {
      if ($testResult->{'stderr'} =~ /$pattern/m) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$pattern> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

  return $result;
}


sub compareToBenchmark
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    my $result;
    my @SQLQuery = @{$testCmd->{'queries'}};
    
    if ($#SQLQuery == 0) {
        $result = $self->compareSingleOutput($testResult, $testResult->{'output'},
                $benchmarkResult->{'output'}, $log);
    } else {
        my $res = 0;
        for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
            my $testOutput = ($testResult->{'outputs'})->[$id];
            my $benchmarkOutput = ($benchmarkResult->{'outputs'})->[$id];
            $res += $self->compareSingleOutput($testResult, $testOutput,
                                               $benchmarkOutput, $log);
            $result = ($res == ($#SQLQuery + 1)) ? 1 : 0;
        }
    }

    return $result;
}


sub compareSingleOutput
{
    my ($self, $testResult, $testOutput, $benchmarkOutput, $log) = @_;

    # cksum the the two files to see if they are the same
    my ($testChksm, $benchmarkChksm);
    IPC::Run::run((['cat', $testOutput], '|', ['cksum']), \$testChksm,
        $log) or die "$0: error: cannot run cksum on test results\n";
    IPC::Run::run((['cat', $benchmarkOutput], '|', ['cksum']),
        \$benchmarkChksm, $log) or die "$0: error: cannot run cksum on benchmark\n";

    chomp $testChksm;
    chomp $benchmarkChksm;
    print $log "test cksum: $testChksm\nbenchmark cksum: $benchmarkChksm\n";

    my $result;
    if ($testChksm ne $benchmarkChksm) {
        print $log "Test output checksum does not match benchmark checksum\n";
        print $log "Test checksum = <$testChksm>\n";
        print $log "Expected checksum = <$benchmarkChksm>\n";
        print $log "RESULTS DIFFER: vimdiff $testOutput $benchmarkOutput\n";
    } else {
        print $log "Test output matches benchmark file: $benchmarkOutput\n";
        $result = 1;
    }

    # Now, check if the sort order is specified
    if (defined($testResult->{'sortArgs'})) {
        Util::setLocale();
	my @sortChk = ('sort', '-cs');
        push(@sortChk, @{$testResult->{'sortArgs'}});
        push(@sortChk, $testResult->{'originalOutput'});
        print $log "Going to run sort check command: " . join(" ", @sortChk) . "\n";
        IPC::Run::run(\@sortChk, \undef, $log, $log);
	my $sortrc = $?;
        if ($sortrc) {
            print $log "Sort check failed\n";
            $result = 0;
        }
    }

    return $result;
}

###############################################################################
# This method has been copied over from TestDriver to make changes to
# support skipping tests which do not match current execution mode
#
#
# Run all the tests in the configuration file.
# @param testsToRun - reference to array of test groups and ids to run
# @param testsToMatch - reference to array of test groups and ids to match.
# If a test group_num matches any of these regular expressions it will be run.
# @param cfg - reference to contents of cfg file
# @param log - reference to a stream pointer for the logs
# @param dbh - reference database connection
# @param testStatuses- reference to hash of test statuses
# @param confFile - config file name
# @param startat - test to start at.
# @returns nothing
# failed.
#
sub run
{
    my ($self, $testsToRun, $testsToMatch, $cfg, $log, $dbh, $testStatuses,
        $confFile, $startat, $logname ) = @_;
    my $subName  = (caller(0))[3];

    my $msg="";
    my $testDuration=0;
    my $totalDuration=0;
    my $groupDuration=0;

    my $sawstart = !(defined $startat);
    # Rather than make each driver handle our multi-level cfg, we'll flatten
    # the hashes into one for it.
    my %globalHash;

    my $runAll = ((scalar(@$testsToRun) == 0) && (scalar(@$testsToMatch) == 0));

    # Read the global keys
    foreach (keys(%$cfg)) {
        next if $_ eq 'groups';
        $globalHash{$_} = $cfg->{$_};
    }

    # Do the global setup
    $self->globalSetup(\%globalHash, $log);

    # Used in generating Junit XML test report
    my $generateJunitReport=1;
    my $report=0;
    my $properties;
    my $xmlDir;

    if ($generateJunitReport) {
      $properties= new Properties( 0, $globalHash{'propertiesFile'} );

      # For the xml directory, use the default directory from the configuration file
      # unless the directory was specified in the command line
      $xmlDir= $globalHash{'localxmlpathbase'} ."/run".  $globalHash{'UID'};
      if ( $globalHash{'reportdir'} ) {
          $xmlDir = $globalHash{'reportdir'};
      }
    }

    my %groupExecuted;

    # $cfg->{'suite'} needs to be set in bin/miners_test_harness to the name of the test conf file...
    if ($cfg->{'suite'}) { 
      print $log "Beginning suite $cfg->{'suite'} at " . time . ($cfg->{'comment'} ? ", comment: $cfg->{'comment'}" : "") . "\n";
    } else {
      print $log "Beginning suite at " . time . ($cfg->{'comment'} ? ", comment: $cfg->{'comment'}" : "") . "\n";
    }

    foreach my $group (@{$cfg->{'groups'}}) {
        my %groupHash = %globalHash;
        $groupHash{'group'} = $group->{'name'};

        # Read the group keys
        $groupHash{'comment'} = undef; # no inheritance of comments
        foreach (keys(%$group)) {
            next if $_ eq 'tests';
            $groupHash{$_} = $group->{$_};
        }

        print $log "Beginning group $groupHash{'group'} at " . time . ($groupHash{'comment'} ? ", comment: $groupHash{'comment'}" : "") . "\n";

        # Run each test
        foreach my $test (@{$group->{'tests'}}) {
            # Check if we're supposed to run this one or not.
            if (!$runAll) {
                # check if we are supposed to run this test or not.
                my $foundIt = 0;
                foreach (@$testsToRun) {
                    if (/^$groupHash{'group'}(_[0-9]+)?$/) {
                        if (not defined $1) {
                            # In this case it's just the group name, so we'll
                            # run every test in the group
                            $foundIt = 1;
                            last;
                        } else {
                            # maybe, it at least matches the group
                            my $num = "_" . $test->{'num'};
                            if ($num eq $1) {
                                $foundIt = 1;
                                last;
                            }
                        }
                    }
                }
                foreach (@$testsToMatch) {
                    my $protoName = $groupHash{'group'} . "_" .  $test->{'num'};
                    if ($protoName =~ /$_/) {
                        if (not defined $1) {
                            # In this case it's just the group name, so we'll
                            # run every test in the group
                            $foundIt = 1;
                            last;
                        } else {
                            # maybe, it at least matches the group
                            my $num = "_" . $test->{'num'};
                            if ($num eq $1) {
                                $foundIt = 1;
                                last;
                            }
                        }
                    }
                }

                next unless $foundIt;
            }

            # This is a test, so run it.
            my %testHash = %groupHash;
            $testHash{'comment'} = undef; # no inheritance of comments
            foreach (keys(%$test)) {
                $testHash{$_} = $test->{$_};
            }
            my $testName = $testHash{'group'} . "_" . $testHash{'num'};

            if ( $groupExecuted{ $group->{'name'} }== 0 ){
               $groupExecuted{ $group->{'name'} }=1;

               mkpath( [ $xmlDir ] , 1, 0777) if ( ! -e $xmlDir );

               my $filename = $group->{'name'}.".xml";
    	       if ($generateJunitReport) {
                 $report = new TestReport ( $properties, "$xmlDir/$filename" );
                 $report->purge();
               }
            }

            # Have we not reached the starting point yet?
            if (!$sawstart) {
                if ($testName eq $startat) {
                    $sawstart = 1;
                } else {
                    next;
                }
            }

            # Check that this test doesn't depend on an earlier test or tests
            # that failed, or that the test wasn't marked as "ignore".
            # Don't abort if that test wasn't run, just assume the
            # user knew what they were doing and set it up right.
            my $skipThisOne = 0;
            foreach (keys(%testHash)) {
                if (/^depends_on/ && defined($testStatuses->{$testHash{$_}}) &&
                        $testStatuses->{$testHash{$_}} ne $passedStr) {

                    print $log "TEST FAILED DEPENDENCY <$testName> at " . time .
                               ": depended on $testHash{$_} which returned a status of $testStatuses->{$testHash{$_}}\n";

                    $testStatuses->{$testName} = $dependStr;
                    $skipThisOne = 1;
                    last;
                }
                # if the test is not applicable to current execution mode
                # ignore it
                if(/^exectype$/i && $testHash{$_} !~ /$self->{'exectype'}/i)
                {
                    print $log "TEST IGNORED <$testName> at " . time . ". Message: running mode ($self->{'exectype'}) and exectype in test ($testHash{'exectype'}) do not match\n";
                    $testStatuses->{$testName} = $skippedStr;
                    $skipThisOne = 1;
                    last;
                }

                # if the test is marked as 'ignore',
                # ignore it... unless option to ignore the ignore is in force
                if(/^ignore$/i)
                {
                  if($self->{'ignore'} eq 'true')
                  {
                      print $log "TEST IGNORED <$testName> at " . time . ". Message: $testHash{'ignore'}\n";
                      $testStatuses->{$testName} = $skippedStr;
                      $skipThisOne = 1;
                      last;
                  }
                  elsif ($testHash{'ignore'} ne 'false')
                  {
                      print $log "TEST _NOT_ IGNORED <$testName> at " . time . ". Message: $testHash{'ignore'}\n";
                  }
                }
            }

            if ($skipThisOne) {
                printResults($testStatuses, $log, "Results so far");
                next;
            }

            # Check if output comparison should be skipped.
            my $dontCompareThisOne = 0; # true for tests with key 'noverify'
            my $copyResults        = 1; # no need to copy output to local if noverify
            foreach (keys(%testHash)) {

                if(/^noverify$/i )
                {
                    $dontCompareThisOne = 1;
                    $copyResults = 0;
                    last;
                }
            }

          # print $log "Beginning test $testName at " . time . "\n";
            print $log "Beginning test $testName at " . time . ($testHash{'comment'} ? ", comment: $testHash{'comment'}" : "") . "\n";
            my %dbinfo = (
                'testrun_id' => $testHash{'trid'},
                'test_type'  => $testHash{'driver'},
               #'test_file'  => $testHash{'file'},
                'test_file'  => $confFile,
                'test_group' => $testHash{'group'},
                'test_num'   => $testHash{'num'},
            );
            my $beginTime = time;
            my ($testResult, $benchmarkResult);
            eval {
                

                my  @SQLQuery = split /;/, $testHash{'sql'};

                # Throw out the last one if it is just space
                if ($SQLQuery[$#SQLQuery] =~ /^\s+$/) { $#SQLQuery--; }

                # If the last one is a comment, decrement the count
                if ($#SQLQuery > 0 && $SQLQuery[$#SQLQuery] !~ /select/i && $SQLQuery[$#SQLQuery] =~ /--/) {
                    $#SQLQuery--;
                }

                $testHash{'queries'} = \@SQLQuery;

                $testResult = $self->runTest(\%testHash, $log, $copyResults);
                my $endTime = time;
                $testDuration = $endTime - $beginTime;

                $benchmarkResult = $self->generateBenchmark(\%testHash, $log);

                my $result;
                if( $dontCompareThisOne ) {
                    $result = 1;
                    print $log "TEST MARKED NOVERIFY <$testName>\n";
                } else {
		    # implementing: 
		    # Bugzilla Ticket 3850819 - aborted scripts has test counted as failed when using command line verificaitons 
 		    if ((defined %testHash->{'rc'}) && (%testHash->{'rc'} == 0) && ($testResult->{'rc'} != 0)) {   
			die "Test run assumed aborted as 'rc' = 0 expected, but actual 'rc' = $testResult->{'rc'}\n";
		    }

                    $result = $self->compare($testResult, $benchmarkResult, $log, \%testHash);
                }

                my $command = $self->getCommand(\%testHash);
                if ($result) {
                        $msg = "TEST SUCCEEDED <$testName> at " . time . ", command: $command, duration: $testDuration\n";
                        $testStatuses->{$testName} = $passedStr;
                } else {
                        $msg = "TEST FAILED <$testName> at " . time . ", command: $command, duration: $testDuration\n";
                        $testStatuses->{$testName} = $failedStr;
                }
                print $log $msg;


                $dbinfo{'duration'} = $testDuration;
                $self->recordResults($result, $testResult, $benchmarkResult,
                    \%dbinfo, $log);
            };

            if ($@) {
                my $endTime = time;
                print $log "TEST ABORTED <$testName> at " . time . "\n";
                print $log "$0::$subName FAILED: Failed to run test $testName <$@>\n";
                $testStatuses->{$testName} = $abortedStr;
                $testDuration = $endTime - $beginTime;
                $dbinfo{'duration'} = $testDuration;
            }

            eval {
                $dbinfo{'status'} = $testStatuses->{$testName};
                if($dbh) {
                    $dbh->insertTestCase(\%dbinfo);
                }
            };
            if ($@) {
                chomp $@;
                warn "Failed to insert test case info, error <$@>\n";
            }

            $self->cleanup($testStatuses->{$testName}, \%testHash, $testResult,
                $benchmarkResult, $log);

    	    if ($generateJunitReport) {
              $report->testcase( $group->{'name'}, $testName, $testDuration, $msg, $testStatuses->{$testName} ) if ( $report );
            }

            $groupDuration = $groupDuration + $testDuration;
            $totalDuration = $totalDuration + $testDuration;
            printResults($testStatuses, $log, "Results so far");
        }
        if ($generateJunitReport &&  $report ) {
            my $reportname= $group->{'name'};
            if ( $globalHash{'reportname'} ) {
                 $reportname= $globalHash{'reportname'};
            }
            # $report->systemOut( $logname, $reportname );
            printGroupResultsXml( $report, $group->{'name'}, $testStatuses, $groupDuration );
       }
       $report = 0;
       $groupDuration=0;

    }

    # Do the global cleanup
    $self->globalCleanup(\%globalHash, $log);
}

##############################################################################
#  Sub: printGroupResultsXml
#  Print the results for the group using junit xml schema using values from the testStatuses hash.
#
# Paramaters:
# $report       - the report object to use to generate the report
# $groupName    - the name of the group to report totals for
# $testStatuses - the hash containing the results for the tests run so far
# $totalDuration- The total time it took to run the group of tests
#
# Returns:
# None.
#
sub printGroupResultsXml
{
        my ( $report, $groupName, $testStatuses,  $totalDuration) = @_;
        $totalDuration=0 if  ( !$totalDuration );

        my ($pass, $fail, $abort, $depend) = (0, 0, 0, 0);

        foreach my $key (keys(%$testStatuses)) {
              if ( $key =~ /^$groupName/ ){
                ($testStatuses->{$key} eq $passedStr) && $pass++;
                ($testStatuses->{$key} eq $failedStr) && $fail++;
                ($testStatuses->{$key} eq $abortedStr) && $abort++;
                ($testStatuses->{$key} eq $dependStr) && $depend++;
               }
        }

        my $total= $pass + $fail + $abort;
        $report->totals( $groupName, $total, $fail, $abort, $totalDuration );

}


1;

