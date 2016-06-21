'''
Salt module to generate salt-cloud config from Pillar data.

:author: Mikael Knutsson
:config: This module expects certain data from Salt Pillar.

This certain data looks something like this:

    defaults:
      providers:
        default_servers: <default amount>
        rename_on_destroy: <bool>
        ssh_interface: <private_ips|public_ips>
        ssh_username: <username>
      profiles:
        del_root_vol_on_destroy: <bool>
        del_all_vols_on_destroy: <bool>
        sync_after_install: <as in salt-cloud>
      mappings:
        minion:
          master: <master FQDN>

    providers:
      <provider name>:
        id: <as in salt-cloud>
        key: <as in salt-cloud>
        keyname: <as in salt-cloud>
        private_key: <as in salt-cloud>
        provider: <as in salt-cloud>
        location: <as in salt-cloud>
        subnets:
          <environment name>:
            - A: <ID>
            - B: <ID>
          <environment name>:
            - A: <ID>
            - B: <ID>
        images:
          default: <AMI ID>
        security_groups:
          common: <ID>
          <name>: <ID>
          <name>:
            - <ID>
            - <ID>
        sizes:
          default: <EC2 instance type>
          <role>: c4.2xlarge
          <role>: t2.medium
        volumes:
          <role>:
            - { size: <size in GB> , device: <dev path>, type: <EBS type> }

    servers:
      <provider name>:
        <environment name>:
          - <role>
          - <role>:
            servers: <default amount override>
            size: <size override>
            volumes:
              - { size: <size in GB> , device: <dev path>, type: <EBS type> , kms_key_id: <ID>}
            interfaces:
              - <extra interface>
              - <extra interface>
            iam_profile: <IAM ID>
        <environment name>:
          - <role>
          - <role>

This module also expects the server name to be in the follownig form:
    <role><2-digit ordinal>.<environment>.<location>.<domain>.<tld>
e.g. mysql01.test.dc1.example.com
'''

import random
import itertools
import logging
import copy


LOG = logging.getLogger(__name__)


def __virtual__():
    return 'simplecloud'


def consume_map(in_providers, in_servers, cloud_defaults):
    providers = {}
    profiles = {}
    maps = {}

    environments = {}
    server_roles = {}
    profiles = {}
    no_of_servers = 0

    if 'profiles' in cloud_defaults:
        server_roles['default'] = cloud_defaults['profiles'].copy()

    for provider_name, prov in in_providers.items():
        provider = {}
        if 'providers' in cloud_defaults:
            provider = cloud_defaults['providers'].copy()

        for env, subnets in prov['subnets'].items():
            environments[env] = {
                'availability_zones': [s.keys().pop() for s in subnets],
                'subnets': {s.keys().pop(): s.values().pop() for s in subnets}
            }
        LOG.info('Environments: %s' % environments)
        del(prov['subnets'])
        # grab all params for profiles
        for server_role, size in prov['sizes'].items():
            update_server(server_roles, server_role, {'size': size})
        del(prov['sizes'])
        for server_role, image in prov['images'].items():
            update_server(server_roles, server_role, {'image': image})
        del(prov['images'])
        for server_role, volumes in prov['volumes'].items():
            update_server(server_roles, server_role, {'volumes': volumes})
        del(prov['volumes'])
        for server_role, sec_groups in prov['security_groups'].items():
            update_server(server_roles, server_role, {'security_groups':
                                                      sec_groups})
        del(prov['security_groups'])
        provider.update(prov)
        providers[provider_name] = provider

    role_defaults = server_roles['default'].copy()

    for pname, provider in in_servers.items():
        if pname not in providers:
            LOG.warn('No provider with name %s in %s.', pname, providers.keys())
            continue
        no_of_servers = providers[pname]['default_servers']
        del(providers[pname]['default_servers'])

        for ename, environment in provider.items():
            if ename not in environments:
                LOG.warn('No env with name %s in %s.', ename, environments.keys())
                continue
            for server_role in environment:
                profile, server_role = _produce_profile(
                    server_role, role_defaults, server_roles, ename)
                if not profile:
                    LOG.warn('Profile %s not valid', server_role)
                    continue
                profile['provider'] = pname
                profile['tag'] = _get_tags(ename, server_role)
                start_servers = (no_of_servers if 'servers' not in profile
                                 else profile['servers'])

                map_profiles = {}
                for az_name in environments[ename]['availability_zones']:
                    if 'interfaces' in profile:
                        profile['network_interfaces'] = []
                        _add_interfaces(profile['network_interfaces'],
                                        profile['interfaces'],
                                        environments,
                                        az_name,
                                        profile['security_groups'])
                    else:
                        profile['network_interfaces'] = [
                            _get_network_interface(
                                0,
                                environments[ename]['subnets'][az_name],
                                profile['security_groups']
                            )
                        ]
                    profile_name = '%(type)s_%(env)s_%(region)s' % {
                        'type': server_role,
                        'env': ename,
                        'region': pname + az_name
                    }
                    if ename not in profiles:
                        profiles[ename] = {}
                    finished_profile = copy.deepcopy(profile)
                    del(finished_profile['security_groups'])
                    if 'servers' in finished_profile:
                        del(finished_profile['servers'])
                    if 'interfaces' in finished_profile:
                        del(finished_profile['interfaces'])
                    profiles[ename][profile_name] = finished_profile
                    map_profiles[az_name] = profile_name
                fqdn = '%(role)s%%02d.%(env)s.%(location)s.example.com' % {
                    'role': server_role,
                    'env': ename,
                    'location': pname
                }
                if ename not in maps:
                    maps[ename] = {}
                maps[ename].update(_get_map_data(
                    map_profiles,
                    start_servers,
                    cloud_defaults['mappings'],
                    fqdn
                ))
    return (providers, profiles, maps)


def _get_cycle_list(seed, in_list):
    random.seed(seed)
    first = random.choice(in_list)
    in_list.remove(first)
    in_list.insert(0, first)
    return in_list


def _get_map_data(profiles, no_of_servers, map_defaults, fqdn):
    return_profiles = {}
    cycle_list = _get_cycle_list(fqdn, profiles.keys())
    i = 1
    for az in itertools.cycle(cycle_list):
        if i > no_of_servers:
            break
        if not profiles[az] in return_profiles:
            return_profiles[profiles[az]] = {}
        return_profiles[profiles[az]].update({fqdn % i: map_defaults})
        i += 1
    return return_profiles


def update_server(server_roles, server_role, values):
    if server_role not in server_roles:
        server_roles[server_role] = {}
    server_roles[server_role].update(values)


def _get_tags(environment, role):
    return {
        'Environment': environment,
        'Role': role
    }


def _add_interfaces(add_to, interfaces, envs, az_name, sec_groups):
    # TODO if not dict in interfaces, map to the same az that we're in
    index = 0
    for env_name in interfaces:
        az_ifaces = None
        if isinstance(env_name, dict):
            interface = env_name
            env_name = interface.keys().pop()
            az_ifaces = interface.values().pop()

        subnet = envs[env_name]['subnets'][az_name]
        iface = _get_network_interface(index, subnet, sec_groups)
        if az_ifaces:
            if '-' in az_ifaces[az_name]:
                iface['NetworkInterfaceId'] = az_ifaces[az_name]
            else:
                iface['PrivateIpAddress'] = az_ifaces[az_name]
        add_to.append(iface)
        index += 1


def _get_network_interface(index, subnet_id, security_groups, **kwargs):
    iface_dict = {
        'DeviceIndex': index,
        'SubnetId': subnet_id,
        'SecurityGroupId': security_groups
    }
    iface_dict.update(kwargs)
    return iface_dict


def _produce_profile(server_role, defaults, profile_defs, environment):
    req_parameters = [
        'size',
        'image',
        'security_groups'
    ]

    profile = copy.deepcopy(defaults)
    overrides = {}
    stname = server_role
    if isinstance(server_role, dict):
        stname = server_role.keys().pop()
        overrides = server_role.values().pop()

    LOG.info('Updating profile %s...', stname)
    # TODO: Security groups should be a list and default should always be included
    if stname in profile_defs:
        profile.update(copy.deepcopy(profile_defs[stname]))
    if 'common' in profile_defs and 'security_groups' in profile_defs['common']:
        common_sec = profile_defs['common']['security_groups']
    profile.update(overrides)

    default_vol_tags = {
        'Environment': environment,
        'Role': stname,
        'Service': 'ebs'
    }

    if 'volumes' in profile:
        for vol in profile['volumes']:
            if 'tags' not in vol:
                vol['tags'] = {}
            vol['tags'].update(copy.deepcopy(default_vol_tags))
            if 'type' in vol:
                vol['tags']['VolumeType'] = vol['type']

    if 'security_groups' not in profile:
        profile['security_groups'] = []
    if not isinstance(profile['security_groups'], list):
        profile['security_groups'] = [profile['security_groups']]
    profile['security_groups'].append(common_sec)
    if not all(reqs in profile for reqs in req_parameters):
        LOG.warn('Server type %s does not have %s defined, it only has %s...',
                 stname,
                 req_parameters,
                 profile)
        return False

    return profile, stname
