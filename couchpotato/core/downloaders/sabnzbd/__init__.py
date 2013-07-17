from .main import Sabnzbd

def start():
    return Sabnzbd()

config = [{
    'name': 'sabnzbd',
    'groups': [
        {
            'tab': 'downloaders',
            'list': 'download_providers',
            'name': 'sabnzbd',
            'label': 'Sabnzbd',
            'description': 'Use <a href="http://sabnzbd.org/" target="_blank">SABnzbd</a> (0.7+) to download NZBs.',
            'wizard': True,
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                    'radio_group': 'nzb',
                },
                {
                    'name': 'host',
                    'default': 'localhost:8080',
                },
                {
                    'name': 'api_key',
                    'label': 'Api Key',
                    'description': 'Used for all calls to Sabnzbd.',
                },
                {
                    'name': 'category',
                    'label': 'Category',
                    'description': 'The category CP places the nzb in. Like <strong>movies</strong> or <strong>couchpotato</strong>',
                },
                {
                    'name': 'replace_folder',
                    'label': 'Replace folder base',
                    'advanced': True,
                    'placeholder': 'Example: /home/, X:\\',
                    'description': 'Replace the first folder base with the second in downloaded movie paths. Use if the downloader is on a different computer to convert the paths.',
                },
                {
                    'name': 'priority',
                    'label': 'Priority',
                    'type': 'dropdown',
                    'default': '0',
                    'advanced': True,
                    'values': [('Paused', -2), ('Low', -1), ('Normal', 0), ('High', 1), ('Forced', 2)],
                    'description': 'Add to the queue with this priority.',
                },
                {
                    'name': 'manual',
                    'default': False,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Disable this downloader for automated searches, but use it when I manually send a release.',
                },
                {
                    'name': 'remove_complete',
                    'advanced': True,
                    'label': 'Remove NZB',
                    'default': False,
                    'type': 'bool',
                    'description': 'Remove the NZB from history after it completed.',
                },
                {
                    'name': 'delete_failed',
                    'default': True,
                    'advanced': True,
                    'type': 'bool',
                    'description': 'Delete a release after the download has failed.',
                },
            ],
        }
    ],
}]
