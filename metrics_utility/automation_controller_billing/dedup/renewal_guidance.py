"""Deduplication implementations for the Renewal Guidance report."""

import pandas as pd


class BaseDedupRenewal:
    """Base class for renewal guidance deduplication containing common functionality."""

    def __init__(self, dataframes, extra_params):
        self.dataframe = dataframes['host_metric'].build_dataframe()
        self.extra_params = extra_params

    @staticmethod
    def _value_positions(column):
        """Map each non-null value in ``column`` to its ascending row positions."""
        values = column.to_numpy()
        positions = {}
        for position in column.notna().to_numpy().nonzero()[0]:
            positions.setdefault(values[position], []).append(int(position))
        return positions

    def _cleanup_null_values(self):
        """Clean up null-like values in key fields."""
        # Cleanup ansible_host_variable
        self.dataframe['ansible_host_variable'] = self.dataframe['ansible_host_variable'].replace('', None)

        # Cleanup ansible_product_serial
        self.dataframe['ansible_product_serial'] = self.dataframe['ansible_product_serial'].replace('NA', None).replace('', None)

        # Cleanup ansible_machine_id
        self.dataframe['ansible_machine_id'] = self.dataframe['ansible_machine_id'].replace('NA', None).replace('', None)

    def _get_latest_hostname(self, dupes):
        """Get the latest non-deleted hostname to represent the duplicate group."""
        if len(dupes) == 1:
            return dupes['hostname'].iat[0]
        return dupes.sort_values(by=['deleted', 'last_automation'], ascending=[True, False])['hostname'].iloc[0]

    def _build_deduped_record(self, dupes, latest_hostname, dupes_clean=None):
        """Build a standardized deduped record from duplicate group."""
        if dupes_clean is None:
            dupes_clean = dupes

        return {
            'hostname': latest_hostname,
            'hostmetric_record_count': dupes['hostname'].nunique(),
            'hostmetric_record_count_active': (dupes[~dupes['deleted']]['hostname'].nunique()),
            'hostmetric_record_count_deleted': (dupes[dupes['deleted']]['hostname'].nunique()),
            'hostnames': self.stringify(set(dupes['hostname'])),
            'ansible_host_variables': self.stringify(set(dupes['ansible_host_variable'])),
            'ansible_product_serials': self.stringify(set(dupes_clean['ansible_product_serial'])),
            'ansible_machine_ids': self.stringify(set(dupes_clean['ansible_machine_id'])),
            'deleted': min(dupes['deleted']),
            'first_automation': min(dupes['first_automation']),
            'last_automation': max(dupes['last_automation']),
            'automated_counter': sum(dupes['automated_counter']),
            'deleted_counter': sum(dupes['deleted_counter']),
            'last_deleted': max(dupes['last_deleted']),
        }

    def stringify(self, value):
        """Convert a set of values to a comma-separated string, filtering out None."""
        return ', '.join(v for v in value if isinstance(v, str))

    def run(self):
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError('Subclasses must implement the run method')


class DedupRenewal(BaseDedupRenewal):
    """Original deduplication logic with iterative relationship discovery."""

    # Keys expanded (in this exact order) on every relationship-discovery iteration.
    DUPLICATE_KEYS = ('hostname', 'ansible_host_variable', 'ansible_product_serial', 'ansible_machine_id')

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        self._cleanup_null_values()

        key_values = {key: self.dataframe[key].to_numpy() for key in self.DUPLICATE_KEYS}
        key_positions = {key: self._value_positions(self.dataframe[key]) for key in self.DUPLICATE_KEYS}
        # Null values never match (they are absent from key_positions), so they are skipped
        # before being remembered as expanded. Precomputing the mask keeps that check cheap
        # and avoids putting NaN/pd.NA into the per-group value sets.
        key_is_null = {key: self.dataframe[key].isna().to_numpy() for key in self.DUPLICATE_KEYS}

        index_labels = self.dataframe.index.tolist()
        index_values = self.dataframe['index'].to_numpy()
        may_have_duplicate_rows = not self.dataframe['index'].is_unique

        iterations = int(self.extra_params['report_renewal_guidance_dedup_iterations'])

        deduped_list = []
        processed_dupes_index = set()

        for position in range(len(index_labels)):
            # Skip if index is in existing dupes_index
            if index_labels[position] in processed_dupes_index:
                continue

            # Start with hostname matches (null hostnames never match, as before)
            positions = list(key_positions['hostname'].get(key_values['hostname'][position], ()))
            seen = set(positions)
            # Key values already expanded for this group. Expanding a value yields its whole
            # bucket at once, so expanding it again can never contribute new positions.
            expanded_values = {key: set() for key in self.DUPLICATE_KEYS}

            for _i in range(iterations):
                for key in self.DUPLICATE_KEYS:
                    positions_for_key = key_positions[key]
                    values_for_key = key_values[key]
                    null_for_key = key_is_null[key]
                    expanded_for_key = expanded_values[key]

                    # Values of every not-yet-expanded matched row, one hop by this key.
                    matched = set()
                    for current in positions:
                        if null_for_key[current]:
                            continue
                        value = values_for_key[current]
                        if value in expanded_for_key:
                            continue
                        expanded_for_key.add(value)
                        matches = positions_for_key.get(value)
                        if matches:
                            matched.update(matches)
                    matched.difference_update(seen)
                    if matched:
                        # Append in ascending row order to preserve tie-breaking.
                        for match in sorted(matched):
                            seen.add(match)
                            positions.append(match)

            dupes = self.dataframe.iloc[positions]
            if iterations > 0 and may_have_duplicate_rows:
                dupes = dupes.drop_duplicates()

            processed_dupes_index.update(index_values[positions])
            latest_hostname = self._get_latest_hostname(dupes)
            deduped_list.append(self._build_deduped_record(dupes, latest_hostname))

        return {'host_metric': pd.DataFrame(deduped_list)}

    def find_dupes(self, dupes, key, values):
        """Find additional duplicates based on a specific key and values.

        Kept for API compatibility; :meth:`run` no longer calls it because it now uses
        the prebuilt inverted indexes instead of full-DataFrame scans.
        """
        next_iteration_dupes = self.dataframe[self.dataframe[key].notnull() & self.dataframe[key].isin(values)]
        dupes = pd.concat([dupes, next_iteration_dupes]).drop_duplicates().reset_index(drop=True)
        return dupes


class DedupRenewalHostname(BaseDedupRenewal):
    """
    Hostname-based deduplication for renewal guidance that mirrors CCSP logic.
    Uses ansible_host_variable || hostname for deduplication, similar to CCSP.
    """

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        # Ensure required columns exist
        if 'ansible_host_variable' not in self.dataframe.columns:
            self.dataframe['ansible_host_variable'] = None

        self._cleanup_null_values()

        # Apply hostname normalization logic similar to CCSP:
        # ansible_host_variable || hostname
        self.dataframe['normalized_hostname'] = self.dataframe['ansible_host_variable'].fillna(self.dataframe['hostname'])

        normalized_positions = self._value_positions(self.dataframe['normalized_hostname'])
        normalized_values = self.dataframe['normalized_hostname'].to_numpy()
        index_labels = self.dataframe.index.tolist()

        deduped_list = []
        processed_dupes_index = set()

        for position in range(len(index_labels)):
            if index_labels[position] in processed_dupes_index:
                continue

            dupes = self.dataframe.iloc[normalized_positions.get(normalized_values[position], [])]
            processed_dupes_index.update(dupes['index'])

            latest_hostname = self._get_latest_hostname(dupes)
            deduped_list.append(self._build_deduped_record(dupes, latest_hostname))

        return {'host_metric': pd.DataFrame(deduped_list)}


class DedupRenewalExperimental(BaseDedupRenewal):
    """
    Experimental deduplication for renewal guidance that combines hostname-based
    deduplication with serial-based deduplication (product_serial + machine_id).
    Mimics the CCSP experimental approach with proper multiple serial handling.
    """

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        # Step 1: Apply hostname-based deduplication first
        # Create a mock dataframe object that returns our dataframe
        class MockDataframe:
            def __init__(self, dataframe):
                self.dataframe = dataframe

            def build_dataframe(self):
                return self.dataframe

        mock_dataframe = MockDataframe(self.dataframe)

        hostname_dedup = DedupRenewalHostname({'host_metric': mock_dataframe}, self.extra_params)
        hostname_result = hostname_dedup.run()
        hostname_df = hostname_result['host_metric']

        # Step 2: Apply serial-based deduplication on top of hostname results
        return self._apply_serial_deduplication(hostname_df)

    def _apply_serial_deduplication(self, hostname_df):
        """Apply serial-based deduplication similar to CCSP experimental mode."""
        hostname_df = hostname_df.copy()
        expanded_df = self._expand_serial_records(hostname_df)
        if expanded_df is None or expanded_df.empty:
            return {'host_metric': hostname_df}

        serial_groups, processed_hostname_groups = self._create_serial_groups(expanded_df)
        final_deduped_list = self._build_final_deduped_list(hostname_df, serial_groups, processed_hostname_groups)
        return {'host_metric': pd.DataFrame(final_deduped_list)}

    def _expand_serial_records(self, hostname_df):
        """Expand aggregated serial data back to individual records for processing."""
        hostname_positions = self._value_positions(self.dataframe['hostname'])
        expanded_records = []
        for _, group_row in hostname_df.iterrows():
            hostnames_in_group = [h.strip() for h in group_row['hostnames'].split(',') if h.strip()]
            positions = set()
            for hostname in hostnames_in_group:
                positions.update(hostname_positions.get(hostname, ()))
            original_records = self.dataframe.iloc[sorted(positions)]
            original_data = group_row.to_dict()
            for _, orig_row in original_records.iterrows():
                # Parse multiple serial numbers from comma-separated values
                product_serials = self._parse_multiple_serials(orig_row.get('ansible_product_serial'))
                machine_ids = self._parse_multiple_serials(orig_row.get('ansible_machine_id'))

                # Create records for all combinations of serials
                serial_combinations = self._create_serial_combinations(product_serials, machine_ids)

                for serial_info in serial_combinations:
                    expanded_records.append(
                        {
                            'hostname': orig_row['hostname'],
                            'hostname_group': group_row['hostname'],
                            'product_serial': serial_info['product_serial'],
                            'machine_id': serial_info['machine_id'],
                            'compound_serial': serial_info['compound_serial'],
                            'individual_serials': serial_info['individual_serials'],
                            'original_data': original_data,
                        }
                    )
        if not expanded_records:
            return None
        return pd.DataFrame(expanded_records)

    def _parse_multiple_serials(self, value):
        """Parse multiple serial numbers from comma-separated values."""
        if pd.isna(value) or value in ['', 'NA']:
            return []

        # Handle single value or comma-separated values
        if isinstance(value, str):
            serials = [s.strip() for s in value.split(',') if s.strip() and s.strip() != 'NA']
            return [s for s in serials if s]  # Remove empty strings

        return [value] if value else []

    def _create_serial_combinations(self, product_serials, machine_ids):
        """Create all valid serial combinations for deduplication."""
        combinations = []

        # If we have both product serials and machine IDs, create compound keys
        if product_serials and machine_ids:
            for ps in product_serials:
                for mid in machine_ids:
                    combinations.append(
                        {
                            'product_serial': ps,
                            'machine_id': mid,
                            'compound_serial': f'{ps}/{mid}',
                            'individual_serials': [ps, mid],
                        }
                    )

        # Also add individual serials for partial matching
        for ps in product_serials:
            combinations.append(
                {
                    'product_serial': ps,
                    'machine_id': None,
                    'compound_serial': f'ps:{ps}',
                    'individual_serials': [ps],
                }
            )

        for mid in machine_ids:
            combinations.append(
                {
                    'product_serial': None,
                    'machine_id': mid,
                    'compound_serial': f'mid:{mid}',
                    'individual_serials': [mid],
                }
            )

        # If no serials, return empty list
        if not combinations:
            combinations.append(
                {
                    'product_serial': None,
                    'machine_id': None,
                    'compound_serial': None,
                    'individual_serials': [],
                }
            )

        return combinations

    def _create_serial_groups(self, expanded_df):
        """Create serial-based groupings with support for multiple serials."""
        serial_groups = {}
        processed_hostname_groups = set()

        self._group_by_compound_serial(expanded_df, serial_groups, processed_hostname_groups)
        self._group_by_individual_serial(expanded_df, serial_groups, processed_hostname_groups)

        return serial_groups, processed_hostname_groups

    def _group_by_compound_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        """Helper to group by compound serials."""
        # Single pass: compound_serial -> hostname groups in first-seen order, deduplicated
        # with a dict as an ordered set (list membership would be quadratic when many groups
        # share one serial).
        groups_by_compound_serial = {}
        for compound_serial, hostname_group in zip(expanded_df['compound_serial'], expanded_df['hostname_group']):
            if pd.isna(compound_serial):
                continue
            groups_by_compound_serial.setdefault(compound_serial, {})[hostname_group] = None

        for compound_serial, hostname_groups in groups_by_compound_serial.items():
            if len(hostname_groups) > 1:
                hostname_groups_in_serial = list(hostname_groups)
                canonical_group = hostname_groups_in_serial[0]
                serial_groups[compound_serial] = {
                    'canonical_group': canonical_group,
                    'groups_to_merge': hostname_groups_in_serial,
                    'serial_type': 'compound',
                }
                processed_hostname_groups.update(hostname_groups_in_serial)

    def _group_by_individual_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        """Helper to group by individual serials."""
        # Single pass: individual serial -> hostname groups in first-seen order, deduplicated
        # with a dict as an ordered set (list membership would be quadratic when many groups
        # share one serial).
        groups_by_serial = {}
        for hostname_group, individual_serials in zip(expanded_df['hostname_group'], expanded_df['individual_serials']):
            for serial in individual_serials:
                if not serial:
                    continue
                groups_by_serial.setdefault(serial, {})[hostname_group] = None

        # `processed_hostname_groups` grows while iterating, so keep the original row order.
        for hostname_group, individual_serials in zip(expanded_df['hostname_group'], expanded_df['individual_serials']):
            if hostname_group in processed_hostname_groups:
                continue
            for serial in individual_serials:
                if not serial:
                    continue
                self._record_individual_serial(serial, groups_by_serial[serial], serial_groups, processed_hostname_groups)

    @staticmethod
    def _record_individual_serial(serial, hostname_groups, serial_groups, processed_hostname_groups):
        serial_key = f'individual:{serial}'
        if len(hostname_groups) <= 1 or serial_key in serial_groups:
            return
        groups = list(hostname_groups)
        serial_groups[serial_key] = {'canonical_group': groups[0], 'groups_to_merge': groups, 'serial_type': 'individual'}
        processed_hostname_groups.update(groups)

    def _build_final_deduped_list(self, hostname_df, serial_groups, processed_hostname_groups):
        """Build the final deduped list based on serial and hostname groups."""
        final_deduped_list = []
        canonical_serial_groups = self._index_canonical_serial_groups(serial_groups)
        hostname_positions = self._value_positions(hostname_df['hostname'])
        for _, row in hostname_df.iterrows():
            hostname_group = row['hostname']
            if hostname_group in processed_hostname_groups:
                self._append_canonical_group(
                    hostname_group,
                    hostname_df,
                    canonical_serial_groups,
                    hostname_positions,
                    final_deduped_list,
                )
            else:
                final_deduped_list.append(row.to_dict())
        return final_deduped_list

    def _index_canonical_serial_groups(self, serial_groups):
        """Map each canonical hostname group to its first matching serial group."""
        canonical_serial_groups = {}
        for serial_info in serial_groups.values():
            canonical_serial_groups.setdefault(serial_info['canonical_group'], serial_info)
        return canonical_serial_groups

    def _append_canonical_group(
        self,
        hostname_group,
        hostname_df,
        canonical_serial_groups,
        hostname_positions,
        final_deduped_list,
    ):
        serial_info = canonical_serial_groups.get(hostname_group)
        if serial_info is not None:
            merged_data = self._merge_hostname_groups(hostname_df, serial_info['groups_to_merge'], hostname_positions)
            final_deduped_list.append(merged_data)
        # else: skip non-canonical processed groups

    def _merge_hostname_groups(self, hostname_df, groups_to_merge, hostname_positions=None):
        """Merge multiple hostname groups into a single group."""
        if hostname_positions is None:
            hostname_positions = self._value_positions(hostname_df['hostname'])

        positions = set()
        for hostname_group in groups_to_merge:
            positions.update(hostname_positions.get(hostname_group, ()))
        groups_data = hostname_df.iloc[sorted(positions)]

        if groups_data.empty:
            return {}

        # Take the latest hostname as representative
        latest_group = groups_data.sort_values(by=['last_automation'], ascending=[False]).iloc[0]

        # Merge the data with improved serial handling
        merged = {
            'hostname': latest_group['hostname'],
            'hostmetric_record_count': (groups_data['hostmetric_record_count'].sum()),
            'hostmetric_record_count_active': (groups_data['hostmetric_record_count_active'].sum()),
            'hostmetric_record_count_deleted': (groups_data['hostmetric_record_count_deleted'].sum()),
            'hostnames': ', '.join([h for group in groups_data['hostnames'] for h in group.split(', ') if h]),
            'ansible_host_variables': ', '.join([h for group in groups_data['ansible_host_variables'] for h in group.split(', ') if h]),
            'ansible_product_serials': self._merge_serial_fields(groups_data['ansible_product_serials']),
            'ansible_machine_ids': self._merge_serial_fields(groups_data['ansible_machine_ids']),
            'deleted': groups_data['deleted'].min(),
            'first_automation': groups_data['first_automation'].min(),
            'last_automation': groups_data['last_automation'].max(),
            'automated_counter': groups_data['automated_counter'].sum(),
            'deleted_counter': groups_data['deleted_counter'].sum(),
            'last_deleted': groups_data['last_deleted'].max(),
        }

        return merged

    def _merge_serial_fields(self, serial_fields):
        """Merge serial fields while preserving individual serial numbers."""
        all_serials = set()
        for field in serial_fields:
            if field:
                # Split comma-separated values and add to set
                serials = [s.strip() for s in str(field).split(', ') if s.strip()]
                all_serials.update(serials)

        # Remove None and empty values, sort for consistency
        clean_serials = sorted([s for s in all_serials if s and s != 'None'])
        return ', '.join(clean_serials) if clean_serials else ''
