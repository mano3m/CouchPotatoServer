from couchpotato import get_session, md5
from couchpotato.api import addApiView
from couchpotato.core.event import fireEvent, addEvent
from couchpotato.core.helpers.encoding import ss, toUnicode
from couchpotato.core.helpers.variable import getTitle
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin
from couchpotato.core.plugins.scanner.main import Scanner
from couchpotato.core.settings.model import File, Release as Relea, Media, \
    ReleaseInfo
from couchpotato.environment import Env
from inspect import ismethod, isfunction
from sqlalchemy.exc import InterfaceError
from sqlalchemy.orm import joinedload_all
from sqlalchemy.sql.expression import and_, or_
import os
import time
import traceback

log = CPLog(__name__)


class Release(Plugin):

    def __init__(self):
        addEvent('release.add', self.add)

        addApiView('release.manual_download', self.manualDownload, docs = {
            'desc': 'Send a release manually to the downloaders',
            'params': {
                'id': {'type': 'id', 'desc': 'ID of the release object in release-table'}
            }
        })
        addApiView('release.delete', self.deleteView, docs = {
            'desc': 'Delete releases',
            'params': {
                'id': {'type': 'id', 'desc': 'ID of the release object in release-table'}
            }
        })
        addApiView('release.ignore', self.ignore, docs = {
            'desc': 'Toggle ignore, for bad or wrong releases',
            'params': {
                'id': {'type': 'id', 'desc': 'ID of the release object in release-table'}
            }
        })
        addApiView('release.for_movie', self.forMovieView, docs = {
            'desc': 'Returns all releases for a movie. Ordered by score(desc)',
            'params': {
                'id': {'type': 'id', 'desc': 'ID of the movie'}
            }
        })

        addEvent('release.download', self.download)
        addEvent('release.try_download_result', self.tryDownloadResult)
        addEvent('release.create_from_search', self.createFromSearch)
        addEvent('release.for_movie', self.forMovie)
        addEvent('release.delete', self.delete)
        addEvent('release.clean', self.clean)
        addEvent('release.update_status', self.updateStatus)

        # Clean releases that didn't have activity in the last week
        addEvent('app.load', self.cleanDone)
        fireEvent('schedule.interval', 'movie.clean_releases', self.cleanDone, hours = 4)

        # Enable / disable interval
        addEvent('setting.save.renamer.enabled.after', self.setCrons)
        addEvent('setting.save.renamer.run_every.after', self.setCrons)
        addEvent('release.check_snatched', self.checkSnatched)

        addEvent('release.has_tag', self.hastagRelease)
        addEvent('release.tag', self.tagRelease)
        addEvent('release.untag', self.untagRelease)

    def setCrons(self):

        fireEvent('schedule.remove', 'release.check_snatched')
        if self.isEnabled() and Env.setting('run_every', section = 'renamer') > 0:
            fireEvent('schedule.interval', 'release.check_snatched', self.checkSnatched, minutes = self.conf('run_every'), single = True)

    def cleanDone(self):

        log.debug('Removing releases from dashboard')

        now = time.time()
        week = 262080

        done_status, available_status, snatched_status, downloaded_status, ignored_status = \
            fireEvent('status.get', ['done', 'available', 'snatched', 'downloaded', 'ignored'], single = True)

        db = get_session()

        # get movies last_edit more than a week ago
        media = db.query(Media) \
            .filter(Media.status_id == done_status.get('id'), Media.last_edit < (now - week)) \
            .all()

        for item in media:
            for rel in item.releases:
                # Remove all available releases
                if rel.status_id in [available_status.get('id')]:
                    fireEvent('release.delete', id = rel.id, single = True)
                # Set all snatched and downloaded releases to ignored to make sure they are ignored when re-adding the move
                elif rel.status_id in [snatched_status.get('id'), downloaded_status.get('id')]:
                    self.updateStatus(id = rel.id, status = ignored_status)

        db.expire_all()

    def add(self, group):

        db = get_session()

        identifier = '%s.%s.%s' % (group['library']['identifier'], group['meta_data'].get('audio', 'unknown'), group['meta_data']['quality']['identifier'])


        done_status, snatched_status = fireEvent('status.get', ['done', 'snatched'], single = True)

        # Add movie
        media = db.query(Media).filter_by(library_id = group['library'].get('id')).first()
        if not media:
            media = Media(
                library_id = group['library'].get('id'),
                profile_id = 0,
                status_id = done_status.get('id')
            )
            db.add(media)
            db.commit()

        # Add Release
        rel = db.query(Relea).filter(
            or_(
                Relea.identifier == identifier,
                and_(Relea.identifier.startswith(group['library']['identifier']), Relea.status_id == snatched_status.get('id'))
            )
        ).first()
        if not rel:
            rel = Relea(
                identifier = identifier,
                movie = media,
                quality_id = group['meta_data']['quality'].get('id'),
                status_id = done_status.get('id')
            )
            db.add(rel)
            db.commit()

        # Add each file type
        added_files = []
        for type in group['files']:
            for cur_file in group['files'][type]:
                added_file = self.saveFile(cur_file, type = type, include_media_info = type is 'movie')
                added_files.append(added_file.get('id'))

        # Add the release files in batch
        try:
            added_files = db.query(File).filter(or_(*[File.id == x for x in added_files])).all()
            rel.files.extend(added_files)
            db.commit()
        except:
            log.debug('Failed to attach "%s" to release: %s', (added_files, traceback.format_exc()))

        fireEvent('media.restatus', media.id)

        return True

    def saveFile(self, filepath, type = 'unknown', include_media_info = False):

        properties = {}

        # Get media info for files
        if include_media_info:
            properties = {}

        # Check database and update/insert if necessary
        return fireEvent('file.add', path = filepath, part = fireEvent('scanner.partnumber', file, single = True), type_tuple = Scanner.file_types.get(type), properties = properties, single = True)

    def deleteView(self, id = None, **kwargs):

        return {
            'success': self.delete(id)
        }

    def delete(self, id):

        db = get_session()

        rel = db.query(Relea).filter_by(id = id).first()
        if rel:
            rel.delete()
            db.commit()
            return True

        return False

    def clean(self, id):

        db = get_session()

        rel = db.query(Relea).filter_by(id = id).first()
        if rel:
            for release_file in rel.files:
                if not os.path.isfile(ss(release_file.path)):
                    db.delete(release_file)
            db.commit()

            if len(rel.files) == 0:
                self.delete(id)

            return True

        return False

    def ignore(self, id = None, **kwargs):

        db = get_session()

        rel = db.query(Relea).filter_by(id = id).first()
        if rel:
            ignored_status, failed_status, available_status = fireEvent('status.get', ['ignored', 'failed', 'available'], single = True)
            self.updateStatus(id, available_status if rel.status_id in [ignored_status.get('id'), failed_status.get('id')] else ignored_status)

        return {
            'success': True
        }

    def manualDownload(self, id = None, **kwargs):

        db = get_session()

        rel = db.query(Relea).filter_by(id = id).first()
        if rel:
            item = {}
            for info in rel.info:
                item[info.identifier] = info.value

            fireEvent('notify.frontend', type = 'release.manual_download', data = True, message = 'Snatching "%s"' % item['name'])

            # Get matching provider
            provider = fireEvent('provider.belongs_to', item['url'], provider = item.get('provider'), single = True)

            if not item.get('protocol'):
                item['protocol'] = item['type']
                item['type'] = 'movie'

            if item.get('protocol') != 'torrent_magnet':
                item['download'] = provider.loginDownload if provider.urls.get('login') else provider.download

            success = self.download(data = item, media = rel.movie.to_dict({
                'profile': {'types': {'quality': {}}},
                'releases': {'status': {}, 'quality': {}},
                'library': {'titles': {}, 'files':{}},
                'files': {}
            }), manual = True)

            if success:
                db.expunge_all()
                rel = db.query(Relea).filter_by(id = id).first() # Get release again @RuudBurger why do we need to get it again??

                fireEvent('notify.frontend', type = 'release.manual_download', data = True, message = 'Successfully snatched "%s"' % item['name'])
            return {
                'success': success
            }
        else:
            log.error('Couldn\'t find release with id: %s', id)

        return {
            'success': False
        }

    def download(self, data, media, manual = False):

        if not data.get('protocol'):
            data['protocol'] = data['type']
            data['type'] = 'movie'

        # Test to see if any downloaders are enabled for this type
        downloader_enabled = fireEvent('download.enabled', manual, data, single = True)

        if downloader_enabled:
            snatched_status, done_status, active_status = fireEvent('status.get', ['snatched', 'done', 'active'], single = True)

            # Download release to temp
            filedata = None
            if data.get('download') and (ismethod(data.get('download')) or isfunction(data.get('download'))):
                filedata = data.get('download')(url = data.get('url'), nzb_id = data.get('id'))
                if filedata == 'try_next':
                    return filedata

            download_result = fireEvent('download', data = data, media = media, manual = manual, filedata = filedata, single = True)
            log.debug('Downloader result: %s', download_result)

            if download_result:
                try:
                    # Mark release as snatched
                    db = get_session()
                    rls = db.query(Relea).filter_by(identifier = md5(data['url'])).first()
                    if rls:
                        renamer_enabled = Env.setting('enabled', 'renamer')

                        # Save download-id info if returned
                        if isinstance(download_result, dict):
                            for key in download_result:
                                rls_info = ReleaseInfo(
                                    identifier = 'download_%s' % key,
                                    value = toUnicode(download_result.get(key))
                                )
                                rls.info.append(rls_info)
                            db.commit()

                        log_movie = '%s (%s) in %s' % (getTitle(media['library']), media['library']['year'], rls.quality.label)
                        snatch_message = 'Snatched "%s": %s' % (data.get('name'), log_movie)
                        log.info(snatch_message)
                        fireEvent('%s.snatched' % data['type'], message = snatch_message, data = rls.to_dict())

                        # If renamer isn't used, mark media done
                        if not renamer_enabled:
                            try:
                                if media['status_id'] == active_status.get('id'):
                                    for profile_type in media['profile']['types']:
                                        if profile_type['quality_id'] == rls.quality.id and profile_type['finish']:
                                            log.info('Renamer disabled, marking media as finished: %s', log_movie)

                                            # Mark release done
                                            self.updateStatus(rls.id, status = done_status)

                                            # Mark media done
                                            mdia = db.query(Media).filter_by(id = media['id']).first()
                                            mdia.status_id = done_status.get('id')
                                            mdia.last_edit = int(time.time())
                                            db.commit()
                            except:
                                log.error('Failed marking media finished, renamer disabled: %s', traceback.format_exc())
                        else:
                            self.updateStatus(rls.id, status = snatched_status)

                except:
                    log.error('Failed marking media finished: %s', traceback.format_exc())

                return True

        log.info('Tried to download, but none of the "%s" downloaders are enabled or gave an error', (data.get('protocol')))

        return False

    def tryDownloadResult(self, results, media, quality_type, manual = False):
        ignored_status, failed_status = fireEvent('status.get', ['ignored', 'failed'], single = True)

        for rel in results:
            if not quality_type.get('finish', False) and quality_type.get('wait_for', 0) > 0 and rel.get('age') <= quality_type.get('wait_for', 0):
                log.info('Ignored, waiting %s days: %s', (quality_type.get('wait_for'), rel['name']))
                continue

            if rel['status_id'] in [ignored_status.get('id'), failed_status.get('id')]:
                log.info('Ignored: %s', rel['name'])
                continue

            if rel['score'] <= 0:
                log.info('Ignored, score to low: %s', rel['name'])
                continue

            downloaded = fireEvent('release.download', data = rel, media = media, manual = manual, single = True)
            if downloaded is True:
                return True
            elif downloaded != 'try_next':
                break

        return False

    def createFromSearch(self, search_results, media, quality_type):

        available_status = fireEvent('status.get', ['available'], single = True)
        db = get_session()

        found_releases = []

        for rel in search_results:

            rel_identifier = md5(rel['url'])
            found_releases.append(rel_identifier)

            rls = db.query(Relea).filter_by(identifier = rel_identifier).first()
            if not rls:
                rls = Relea(
                    identifier = rel_identifier,
                    movie_id = media.get('id'),
                    #media_id = media.get('id'),
                    quality_id = quality_type.get('quality_id'),
                    status_id = available_status.get('id')
                )
                db.add(rls)
            else:
                [db.delete(old_info) for old_info in rls.info]
                rls.last_edit = int(time.time())

            db.commit()

            for info in rel:
                try:
                    if not isinstance(rel[info], (str, unicode, int, long, float)):
                        continue

                    rls_info = ReleaseInfo(
                        identifier = info,
                        value = toUnicode(rel[info])
                    )
                    rls.info.append(rls_info)
                except InterfaceError:
                    log.debug('Couldn\'t add %s to ReleaseInfo: %s', (info, traceback.format_exc()))

            db.commit()

            rel['status_id'] = rls.status_id

        return found_releases

    def forMovie(self, id = None):

        db = get_session()

        releases_raw = db.query(Relea) \
            .options(joinedload_all('info')) \
            .options(joinedload_all('files')) \
            .filter(Relea.movie_id == id) \
            .all()

        releases = [r.to_dict({'info':{}, 'files':{}}) for r in releases_raw]
        releases = sorted(releases, key = lambda k: k['info'].get('score', 0), reverse = True)

        return releases

    def forMovieView(self, id = None, **kwargs):

        releases = self.forMovie(id)

        return {
            'releases': releases,
            'success': True
        }

    def updateStatus(self, id, status = None):
        if not status: return False

        db = get_session()

        rel = db.query(Relea).filter_by(id = id).first()
        if rel and status and rel.status_id != status.get('id'):

            item = {}
            for info in rel.info:
                item[info.identifier] = info.value

            if rel.files:
                for file_item in rel.files:
                    if file_item.type.identifier == 'movie':
                        release_name = os.path.basename(file_item.path)
                        break
            else:
                release_name = item['name']
            #update status in Db
            log.debug('Marking release %s as %s', (release_name, status.get("label")))
            rel.status_id = status.get('id')
            rel.last_edit = int(time.time())
            db.commit()

            #Update all movie info as there is no release update function
            fireEvent('notify.frontend', type = 'release.update_status', data = rel.to_dict())

        return True

    def checkSnatched(self):

        if self.checking_snatched:
            log.debug('Already checking snatched')
            return False

        self.checking_snatched = True

        snatched_status, ignored_status, failed_status, seeding_status, downloaded_status, missing_status = \
            fireEvent('status.get', ['snatched', 'ignored', 'failed', 'seeding', 'downloaded', 'missing'], single = True)

        db = get_session()
        rels = db.query(Release).filter(
            Release.status_id.in_([snatched_status.get('id'), seeding_status.get('id'), missing_status.get('id')])
        ).all()

        if not rels:
            #No releases found that need status checking
            self.checking_snatched = False
            return True

        # Collect all download information with the download IDs from the releases
        download_ids = []
        try:
            for rel in rels:
                rel_dict = rel.to_dict({'info': {}})
                if rel_dict['info'].get('download_id'):
                    download_ids.append({'id': rel_dict['info']['download_id'], 'downloader': rel_dict['info']['download_downloader']})
        except:
            log.error('Error getting download IDs from database')
            return False

        release_downloads = fireEvent('download.status', download_ids, merge = True)
        if not release_downloads:
            log.debug('Download status functionality is not implemented for any active downloaders.')
            fireEvent('renamer.scan')

            self.checking_snatched = False
            return True

        scan_releases = []
        scan_required = False

        log.debug('Checking status snatched releases...')

        try:
            for rel in rels:
                rel_dict = rel.to_dict({'info': {}})
                movie_dict = fireEvent('media.get', media_id = rel.movie_id, single = True)

                if not isinstance(rel_dict['info'], (dict)):
                    log.error('Faulty release found without any info, ignoring.')
                    fireEvent('release.update_status', rel.id, status = ignored_status, single = True)
                    continue

                # Check if download ID is available
                if not rel_dict['info'].get('download_id'):
                    log.debug('Download status functionality is not implemented for downloader (%s) of release %s.', (rel_dict['info'].get('download_downloader', 'unknown'), rel_dict['info']['name']))
                    scan_required = True

                    # Continue with next release
                    continue

                # Find release in downloaders
                nzbname = self.createNzbName(rel_dict['info'], movie_dict)

                for release_download in release_downloads:
                    found_release = False
                    if rel_dict['info'].get('download_id'):
                        if release_download['id'] == rel_dict['info']['download_id'] and release_download['downloader'] == rel_dict['info']['download_downloader']:
                            log.debug('Found release by id: %s', release_download['id'])
                            found_release = True
                            break
                    else:
                        if release_download['name'] == nzbname or rel_dict['info']['name'] in release_download['name'] or getImdb(release_download['name']) == movie_dict['library']['identifier']:
                            log.debug('Found release by release name or imdb ID: %s', release_download['name'])
                            found_release = True
                            break

                if not found_release:
                    log.info('%s not found in downloaders', nzbname)

                    #Check status if already missing and for how long, if > 1 week, set to ignored else to missing
                    if rel.status_id == missing_status.get('id'):
                        if rel.last_edit < int(time.time()) - 7 * 24 * 60 * 60:
                            fireEvent('release.update_status', rel.id, status = ignored_status, single = True)
                    else:
                        # Set the release to missing
                        fireEvent('release.update_status', rel.id, status = missing_status, single = True)

                    # Continue with next release
                    continue

                # Log that we found the release
                timeleft = 'N/A' if release_download['timeleft'] == -1 else release_download['timeleft']
                log.debug('Found %s: %s, time to go: %s', (release_download['name'], release_download['status'].upper(), timeleft))

                # Check status of release
                if release_download['status'] == 'busy':
                    # Set the release to snatched if it was missing before
                    fireEvent('release.update_status', rel.id, status = snatched_status, single = True)

                    # Tag folder if it is in the 'from' folder and it will not be processed because it is still downloading
                    if self.movieInFromFolder(release_download['folder']):
                        fireEvent('release.tag', release_download = release_download, tag = 'downloading')

                elif release_download['status'] == 'seeding':
                    #If linking setting is enabled, process release
                    if self.conf('file_action') != 'move' and not rel.status_id == seeding_status.get('id') and self.statusInfoComplete(release_download):
                        log.info('Download of %s completed! It is now being processed while leaving the original files alone for seeding. Current ratio: %s.', (release_download['name'], release_download['seed_ratio']))

                        # Remove the downloading tag
                        fireEvent('release.untag', release_download = release_download, tag = 'downloading')

                        # Scan and set the torrent to paused if required
                        release_download.update({'pause': True, 'scan': True, 'process_complete': False})
                        scan_releases.append(release_download)
                    else:
                        #let it seed
                        log.debug('%s is seeding with ratio: %s', (release_download['name'], release_download['seed_ratio']))

                        # Set the release to seeding
                        fireEvent('release.update_status', rel.id, status = seeding_status, single = True)

                elif release_download['status'] == 'failed':
                    # Set the release to failed
                    fireEvent('release.update_status', rel.id, status = failed_status, single = True)

                    fireEvent('download.remove_failed', release_download, single = True)

                    if self.conf('next_on_failed'):
                        fireEvent('movie.searcher.try_next_release', media_id = rel.movie_id)

                elif release_download['status'] == 'completed':
                    log.info('Download of %s completed!', release_download['name'])

                    #Make sure the downloader sent over a path to look in
                    if self.statusInfoComplete(release_download):

                        # If the release has been seeding, process now the seeding is done
                        if rel.status_id == seeding_status.get('id'):
                            if self.conf('file_action') != 'move':
                                # Set the release to done as the movie has already been renamed
                                fireEvent('release.update_status', rel.id, status = downloaded_status, single = True)

                                # Allow the downloader to clean-up
                                release_download.update({'pause': False, 'scan': False, 'process_complete': True})
                                scan_releases.append(release_download)
                            else:
                                # Scan and Allow the downloader to clean-up
                                release_download.update({'pause': False, 'scan': True, 'process_complete': True})
                                scan_releases.append(release_download)

                        else:
                            # Set the release to snatched if it was missing before
                            fireEvent('release.update_status', rel.id, status = snatched_status, single = True)

                            # Remove the downloading tag
                            fireEvent('release.untag', release_download = release_download, tag = 'downloading')

                            # Scan and Allow the downloader to clean-up
                            release_download.update({'pause': False, 'scan': True, 'process_complete': True})
                            scan_releases.append(release_download)
                    else:
                        scan_required = True

        except:
            log.error('Failed checking for release in downloader: %s', traceback.format_exc())

        # The following can either be done here, or inside the scanner if we pass it scan_items in one go
        for release_download in scan_releases:
            # Ask the renamer to scan the item
            if release_download['scan']:
                if release_download['pause'] and self.conf('file_action') == 'link':
                    fireEvent('download.pause', release_download = release_download, pause = True, single = True)
                fireEvent('renamer.scan', release_download = release_download)
                if release_download['pause'] and self.conf('file_action') == 'link':
                    fireEvent('download.pause', release_download = release_download, pause = False, single = True)
            if release_download['process_complete']:
                #First make sure the files were succesfully processed
                if not fireEvent('release.has_tag', release_download = release_download, tag = 'failed_rename'):
                    # Remove the seeding tag if it exists
                    fireEvent('release.untag', release_download = release_download, tag = 'renamed_already')
                    # Ask the downloader to process the item
                    fireEvent('download.process_complete', release_download = release_download, single = True)

        if scan_required:
            fireEvent('renamer.scan')

        self.checking_snatched = False
        return True

    def statusInfoComplete(self, release_download):
        return release_download['id'] and release_download['downloader'] and release_download['folder']

    # This adds a file to ignore / tag a release so it is ignored later
    def tagRelease(self, tag, group = None, release_download = None):
        if not tag:
            return

        text = """This file is from CouchPotato
It has marked this release as "%s"
This file hides the release from the renamer
Remove it if you want it to be renamed (again, or at least let it try again)
""" % tag

        tag_files = []

        # Tag movie files if they are known
        if isinstance(group, dict):
            tag_files = [sorted(list(group['files']['movie']))[0]]

        elif isinstance(release_download, dict):
            # Tag download_files if they are known
            if release_download['files']:
                tag_files = splitString(release_download['files'], '|')

            # Tag all files in release folder
            else:
                for root, folders, names in os.walk(release_download['folder']):
                    tag_files.extend([os.path.join(root, name) for name in names])

        for filename in tag_files:

            # Dont tag .ignore files
            if os.path.splitext(filename)[1] == '.ignore':
                continue

            tag_filename = '%s.%s.ignore' % (os.path.splitext(filename)[0], tag)
            if not os.path.isfile(tag_filename):
                self.createFile(tag_filename, text)

    def untagRelease(self, release_download, tag = ''):
        if not release_download:
            return

        tag_files = []

        folder = release_download['folder']
        if not os.path.isdir(folder):
            return False

        # Untag download_files if they are known
        if release_download['files']:
            tag_files = splitString(release_download['files'], '|')

        # Untag all files in release folder
        else:
            for root, folders, names in os.walk(release_download['folder']):
                tag_files.extend([sp(os.path.join(root, name)) for name in names if not os.path.splitext(name)[1] == '.ignore'])

        # Find all .ignore files in folder
        ignore_files = []
        for root, dirnames, filenames in os.walk(folder):
            ignore_files.extend(fnmatch.filter([sp(os.path.join(root, filename)) for filename in filenames], '*%s.ignore' % tag))

        # Match all found ignore files with the tag_files and delete if found
        for tag_file in tag_files:
            ignore_file = fnmatch.filter(ignore_files, fnEscape('%s.%s.ignore' % (os.path.splitext(tag_file)[0], tag if tag else '*')))
            for filename in ignore_file:
                try:
                    os.remove(filename)
                except:
                    log.debug('Unable to remove ignore file: %s. Error: %s.' % (filename, traceback.format_exc()))

    def hastagRelease(self, release_download, tag = ''):
        if not release_download:
            return False

        folder = release_download['folder']
        if not os.path.isdir(folder):
            return False

        tag_files = []
        ignore_files = []

        # Find tag on download_files if they are known
        if release_download['files']:
            tag_files = splitString(release_download['files'], '|')

        # Find tag on all files in release folder
        else:
            for root, folders, names in os.walk(release_download['folder']):
                tag_files.extend([sp(os.path.join(root, name)) for name in names if not os.path.splitext(name)[1] == '.ignore'])

        # Find all .ignore files in folder
        for root, dirnames, filenames in os.walk(folder):
            ignore_files.extend(fnmatch.filter([sp(os.path.join(root, filename)) for filename in filenames], '*%s.ignore' % tag))

        # Match all found ignore files with the tag_files and return True found
        for tag_file in tag_files:
            ignore_file = fnmatch.filter(ignore_files, fnEscape('%s.%s.ignore' % (os.path.splitext(tag_file)[0], tag if tag else '*')))
            if ignore_file:
                return True

        return False

