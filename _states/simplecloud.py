'''
Salt state to generate salt-cloud config from Pillar data using the simplecloud module.

:author: Mikael Knutsson
:config:

    user: Owner of the created files
    group: Groupt owning the created files
    cloud_servers_pillar: The pillar variable that holds server info
    cloud_providers_pillar: The pillar variable that holds info for the providers
    cloud_defaults_pillar: The pillar variable that holds the defaults
    conf_dir: Base salt config directory (default: /etc/salt)
    file_mode: File permissions (default: 0600)
    dir_mode: Directory permissions (default: 0700)

    e.g.:
        cloudfu:
          simplecloud.managed:
            - user: root
            - group: root
            - cloud_servers_pillar: servers
            - cloud_providers_pillar: providers
            - cloud_defaults_pillar: defaults
            - conf_dir: /etc/salt
'''

import os.path
import yaml
from collections import OrderedDict


def __virtual__():
    return 'simplecloud'


class _ExplicitDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def _ordered_dump(data, stream=None, Dumper=_ExplicitDumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)


def managed(
        name,
        conf_dir='/etc/salt',
        cloud_servers_pillar=None,
        cloud_providers_pillar=None,
        cloud_defaults_pillar=None,
        file_mode='0600',
        dir_mode='0700',
        user='root',
        group='root',
        test=False
):
    from salt.states import file
    file.__instance_id__ = str(id(managed))
    file.__env__ = __env__
    file.__opts__ = __opts__
    file.__salt__ = __salt__
    ret = {
        'name': name,
        'result': False,
        'comment': '',
        'changes': {},
    }
    comments = []
    results = []
    changes = {}
    provider_path = os.path.join(conf_dir, 'cloud.providers.d')
    profile_path = os.path.join(conf_dir, 'cloud.profiles.d')
    map_path = os.path.join(conf_dir, 'cloud.maps')

    cloud_servers = __salt__['pillar.get'](cloud_servers_pillar)
    cloud_providers = __salt__['pillar.get'](cloud_providers_pillar)
    cloud_defaults = __salt__['pillar.get'](cloud_defaults_pillar)

    providers, profiles, maps = __salt__['simplecloud.consume_map'](
        cloud_providers,
        cloud_servers,
        cloud_defaults
    )

    provider_dir_results = file.directory(
        provider_path,
        user=user,
        group=group,
        dir_mode=dir_mode,
        test=test
    )
    comments.append(provider_dir_results['comment'])
    results.append(provider_dir_results['result'])
    if provider_dir_results['changes']:
        changes[provider_path] = provider_dir_results['changes']

    profile_dir_results = file.directory(
        profile_path,
        user=user,
        group=group,
        dir_mode=dir_mode,
        test=test
    )
    comments.append(profile_dir_results['comment'])
    results.append(profile_dir_results['result'])
    if profile_dir_results['changes']:
        changes[profile_path] = profile_dir_results['changes']

    map_dir_results = file.directory(
        map_path,
        user=user,
        group=group,
        dir_mode=dir_mode,
        test=test
    )
    comments.append(map_dir_results['comment'])
    results.append(map_dir_results['result'])
    if map_dir_results['changes']:
        changes[map_path] = map_dir_results['changes']

    for provider_name, provider in providers.items():
        provider_data = {provider_name: provider}
        provider_file = os.path.join(provider_path, '%s.conf' % provider_name)
        source = _ordered_dump(provider_data, default_flow_style=False)
        provider_results = file.managed(
            provider_file,
            user=user,
            group=group,
            mode=file_mode,
            contents=source,
            test=test
        )
        comments.append(provider_results['comment'])
        results.append(provider_results['result'])
        if provider_results['changes']:
            changes[provider_file] = provider_results['changes']

    for env_name, profile_data in profiles.items():
        profile_file = os.path.join(profile_path, '%s.conf' % env_name)
        source = _ordered_dump(profile_data, default_flow_style=False)
        profile_results = file.managed(
            profile_file,
            user=user,
            group=group,
            mode=file_mode,
            contents=source,
            test=test
        )
        comments.append(profile_results['comment'])
        results.append(profile_results['result'])
        if profile_results['changes']:
            changes[profile_file] = profile_results['changes']

    for env_name, map_data in maps.items():
        map_file = os.path.join(map_path, env_name)
        source = _ordered_dump(map_data, default_flow_style=False)
        map_results = file.managed(
            map_file,
            user=user,
            group=group,
            mode=file_mode,
            contents=source,
            test=test
        )
        comments.append(map_results['comment'])
        results.append(map_results['result'])
        if map_results['changes']:
            changes[map_file] = map_results['changes']

    ret['changes'] = {s: c for s, c in changes.items() if c}
    ret['comment'] = '\n'.join([c for c in comments if c])
    ret['result'] = all(results)
    return ret
