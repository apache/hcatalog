#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import os.path
import subprocess
import sys

from optparse import OptionParser

def get_snapshot_version():
  with open('build.properties') as f:
    for line in f.readlines():
      if line.startswith('hcatalog.version='):
        return line.strip().split('=')[1]

def update_poms(release_version):
  snapshot_version = get_snapshot_version()
  for root, dirs, files in os.walk(os.getcwd()):
    for filename in files:
      if filename in ['build.properties', 'pom.xml']:
        abs_filename = os.path.join(root, filename)
        lines = []
        with open(abs_filename) as f:
          for line in f:
            lines.append(line.replace(snapshot_version, release_version))
        with open(abs_filename, 'w') as f:
          f.write(''.join(lines))

def main():
  parser = OptionParser(usage=('usage: %prog [options])\n\n'
      'This tool automates HCatalog release-releated tasks, providing '
      'flexibility around the build and deploy process (useful for '
      'site-specific customization). For more information, see '
      'https://cwiki.apache.org/confluence/display/HCATALOG/HowToRelease'))
  parser.add_option('--dry-run',
      dest='dry_run',
      default=False,
      action='store_true',
      help=('Perform a release dry run, which appends `dryrun` to the version '
            'and does not publish artifacts to Maven. (default: %default)'))
  parser.add_option('--release-version',
    dest='release_version',
    default=None,
    help='HCatalog release version. (default: %default)')
  parser.add_option('--forrest-home',
    dest='forrest_home',
    default='/usr/local/apache-forrest-0.9',
    help='Path to the Apache Forrest home. (default: %default)')
  parser.add_option('--mvn-deploy-repo-id',
    dest='mvn_deploy_repo_id',
    default='apache.releases.https',
    help=('Maven repo id to publish to. This id must exist in your '
          'settings.xml, along with your username/password to publish '
          'artifacts. For more information, see '
          'http://maven.apache.org/settings.html#Servers (default: %default)'))
  parser.add_option('--mvn-deploy-repo-url',
    dest='mvn_deploy_repo_url',
    default='https://repository.apache.org/service/local/staging/deploy/maven2',
    help='Maven repo URL to publish to. (default: %default)')
  parser.add_option('--ant-args',
    dest='ant_args',
    default=None,
    help=('Extra args for ant, such as overriding something from '
          '`build.properties`. (default: %default)'))
  (options, args) = parser.parse_args()

  if len(sys.argv) == 0:
    parser.print_help()
    sys.exit(-1)

  if options.release_version is None:
    print('Required option --release-version not set!')
    parser.print_help()
    sys.exit(-1)

  ant_args = []
  if os.environ.has_key('ANT_ARGS'):
    ant_args.append(os.environ['ANT_ARGS'])
  if options.ant_args is not None:
    ant_args.append(options.ant_args)

  if options.dry_run:
    options.release_version = '%s-dryrun' % options.release_version
  else:
    ant_args.extend([
      '-Dmvn.deploy.repo.id=%s' % options.mvn_deploy_repo_id,
      '-Dmvn.deploy.repo.url=%s' % options.mvn_deploy_repo_url,
    ])
    os.environ['HCAT_MVN_DEPLOY'] = 'true'

  os.environ['FORREST_HOME'] = options.forrest_home
  os.environ['ANT_ARGS'] = ' '.join(ant_args)

  update_poms(options.release_version)

  subprocess.Popen(os.path.join(os.path.dirname(__file__), 'test.sh'),
      env=os.environ, stdout=sys.stdout, stderr=sys.stderr).communicate()

if __name__ == '__main__':
  main()
