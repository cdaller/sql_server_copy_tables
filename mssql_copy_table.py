#!/usr/bin/env python3

import pyodbc
from time import perf_counter
import sys

PAGE_SIZE=100000

# Configuration for source and target databases
# PROD
source_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'portal-int-cl1-prod-sqlserver.database.windows.net',
    'database': 'portal-int-cl1-prod-liferay-db',
    'user': 'portal_int_cl1_prod_liferay_portal',
    'password': 'xxx',
    'schema': 'dbo'
}

# local db:
# source_config = {
#     'driver': '{ODBC Driver 18 for SQL Server}',
#     'server': 'localhost',
#     'database': 'liferay-db',
#     'user': 'sa',
#     'password': 'xxx',
#     'schema': 'dbo'
# }

# DEV
# jdbc:sqlserver://portal-int-cl1-dev-sqlserver.database.windows.net:1433;database=portal-int-cl1-dev-liferay-db;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
# target_config = {
#     'driver': '{ODBC Driver 18 for SQL Server}',
#     'server': 'portal-int-cl1-dev-sqlserver.database.windows.net',
#     'database': 'portal-int-cl1-dev-liferay-db',
#     'user': 'portal_int_cl1_dev_liferay_portal2',
#     'password': 'xxx',
#     'schema': 'dbo',
#     'create_table': True,
#     'truncate_table': False,
#     'copy_indices': True
# }

# local db:
target_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'localhost',
    'database': 'liferay-db',
    'user': 'sa',
    'password': 'xxx',
    'schema': 'dbo',
    'create_table': True,
    'drop_table': True,
    'truncate_table': False,
    'copy_indices': True
}

# schema HEIDETRANSFER:
# table_names = [
#             'D7ADRESS',
#             'DH7ANS',
#             'DH7EINH',
#             'DH7MES',
#             'DH7NUTZ',
#             'DH7OBJ',
#             'PORTALFLAG',
#             'REP_CRE_KV',
#             'REP_CRE_MASTER3A', 
#             'REP_CRE_MASTER3B', 
#             'REP_CRE_MASTER3C', 
#             'REP_CRE_MP', 'REP_CRE_OB_KZ', 'REP_CRE_VAZ'
#                ]

# schema dbo:
table_names = [
    # 'AccountEntry',
    # 'AccountEntry_x_40201',
    # 'AccountEntry_x_447240',
    # 'AccountEntryOrganizationRel',
    # 'AccountEntryUserRel',
    # 'AccountGroup',
    # 'AccountGroupRel',
    # 'AccountRole',
    # 'Address',
    # 'Address_x_40201',
    # 'Address_x_447240',
    # 'AMImageEntry',
    # 'AnalyticsAssociation',
    # 'AnalyticsDeleteMessage',
    # 'AnalyticsMessage',
    # 'AnnouncementsDelivery',
    # 'AnnouncementsEntry',
    # 'AnnouncementsFlag',
    # 'AssetAutoTaggerEntry',
    # 'AssetCategory',
    # 'AssetCategoryProperty',
    # 'AssetDisplayPageEntry',
    # 'AssetEntries_AssetTags',
    # 'AssetEntry',
    # 'AssetEntryAssetCategoryRel',
    # 'AssetEntryUsage',
    # 'AssetLink',
    # 'AssetListEntry',
    # 'AssetListEntryAssetEntryRel',
    # 'AssetListEntrySegmentsEntryRel',
    # 'AssetListEntryUsage',
    # 'AssetTag',
    # 'AssetVocabulary',
    # 'Audit_AuditEvent',
    # 'BackgroundTask',
    # 'BatchEngineExportTask',
    # 'BatchEngineImportTask',
    # 'BatchEngineImportTaskError',
    # 'BatchPlannerMapping',
    # 'BatchPlannerPlan',
    # 'BatchPlannerPolicy',
    # 'BlogsEntry',
    # 'BrowserTracker',
    # 'Calendar',
    # 'CalendarBooking',
    # 'CalendarNotificationTemplate',
    # 'CalendarResource',
    # 'CChannelAccountEntryRel',
    # 'CDiscountCAccountGroupRel',
    # 'ChangesetCollection',
    # 'ChangesetEntry',
    # 'CIAudit',
    # 'CIBookedQuantity',
    # 'CIReplenishmentItem',
    # 'CIWarehouse',
    # 'CIWarehouseGroupRel',
    # 'CIWarehouseItem',
    # 'CIWarehouseRel',
    # 'ClassName_',
    # 'ClientExtensionEntry',
    # 'ClientExtensionEntryRel',
    # 'CNotificationAttachment',
    # 'CNTemplateCAccountGroupRel',
    # 'CommerceAddressRestriction',
    # 'CommerceAvailabilityEstimate',
    # 'CommerceCatalog',
    # 'CommerceChannel',
    # 'CommerceChannelRel',
    # 'CommerceCurrency',
    # 'CommerceDiscount',
    # 'CommerceDiscountAccountRel',
    # 'CommerceDiscountOrderTypeRel',
    # 'CommerceDiscountRel',
    # 'CommerceDiscountRule',
    # 'CommerceDiscountUsageEntry',
    # 'CommerceMLForecastAlertEntry',
    # 'CommerceNotificationQueueEntry',
    # 'CommerceNotificationTemplate',
    # 'CommerceOrder',
    # 'CommerceOrder_x_40201',
    # 'CommerceOrder_x_447240',
    # 'CommerceOrderItem',
    # 'CommerceOrderNote',
    # 'CommerceOrderPayment',
    # 'CommerceOrderType',
    # 'CommerceOrderTypeRel',
    # 'CommercePaymentEntry',
    # 'CommercePaymentEntryAudit',
    # 'CommercePaymentMethodGroupRel',
    # 'CommercePriceEntry',
    # 'CommercePriceList',
    # 'CommercePriceListAccountRel',
    # 'CommercePriceListChannelRel',
    # 'CommercePriceListDiscountRel',
    # 'CommercePriceListOrderTypeRel',
    # 'CommercePriceModifier',
    # 'CommercePriceModifierRel',
    # 'CommercePricingClass',
    # 'CommercePricingClass_x_40201',
    # 'CommercePricingClass_x_447240',
    # 'CommerceShipment',
    # 'CommerceShipmentItem',
    # 'CommerceShippingFixedOption',
    # 'CommerceShippingMethod',
    # 'CommerceSubscriptionEntry',
    # 'CommerceTaxFixedRate',
    # 'CommerceTaxFixedRateAddressRel',
    # 'CommerceTaxMethod',
    # 'CommerceTermEntry',
    # 'CommerceTermEntryRel',
    # 'CommerceTierPriceEntry',
    # 'CommerceVirtualOrderItem',
    # 'CommerceWishList',
    # 'CommerceWishListItem',
    'Company',
    # 'CompanyInfo',
    # 'Configuration_',
    # 'Contact_',
    # 'Contacts_Entry',
    # 'COREntry',
    # 'COREntryRel',
    # 'Counter',
    # 'Country',
    # 'CountryLocalization',
    # 'CPAttachmentFileEntry',
    # 'CPDAvailabilityEstimate',
    # 'CPDefinition',
    # 'CPDefinition_x_40201',
    # 'CPDefinition_x_447240',
    # 'CPDefinitionGroupedEntry',
    # 'CPDefinitionInventory',
    # 'CPDefinitionLink',
    # 'CPDefinitionLocalization',
    # 'CPDefinitionOptionRel',
    # 'CPDefinitionOptionValueRel',
    # 'CPDefinitionVirtualSetting',
    # 'CPDisplayLayout',
    # 'CPDSpecificationOptionValue',
    # 'CPInstance',
    # 'CPInstanceOptionValueRel',
    # 'CPInstanceUOM',
    # 'CPLCommerceGroupAccountRel',
    # 'CPMeasurementUnit',
    # 'CPMethodGroupRelQualifier',
    # 'CPOption',
    # 'CPOptionCategory',
    # 'CPOptionValue',
    # 'CPricingClassCPDefinitionRel',
    # 'CProduct',
    # 'CPSpecificationOption',
    # 'CPTaxCategory',
    # 'CSDiagramEntry',
    # 'CSDiagramPin',
    # 'CSDiagramSetting',
    # 'CSFixedOptionQualifier',
    # 'CShippingFixedOptionRel',
    # 'CSOptionAccountEntryRel',
    # 'CTAutoResolutionInfo',
    # 'CTCollection',
    # 'CTCollectionTemplate',
    # 'CTComment',
    # 'CTEntry',
    # 'CTermEntryLocalization',
    # 'CTMessage',
    # 'CTPreferences',
    # 'CTProcess',
    # 'CTRemote',
    # 'CTSchemaVersion',
    # 'CTSContent',
    # 'DCCSKeyValue',
    # 'DDLRecord',
    # 'DDLRecordSet',
    # 'DDLRecordSetVersion',
    # 'DDLRecordVersion',
    # 'DDMContent',
    # 'DDMDataProviderInstance',
    # 'DDMDataProviderInstanceLink',
    # 'DDMField',
    # 'DDMFieldAttribute',
    # 'DDMFormInstance',
    # 'DDMFormInstanceRecord',
    # 'DDMFormInstanceRecordVersion',
    # 'DDMFormInstanceReport',
    # 'DDMFormInstanceVersion',
    # 'DDMStorageLink',
    # 'DDMStructure',
    # 'DDMStructureLayout',
    # 'DDMStructureLink',
    # 'DDMStructureVersion',
    # 'DDMTemplate',
    # 'DDMTemplateLink',
    # 'DDMTemplateVersion',
    # 'DEDataDefinitionFieldLink',
    # 'DEDataListView',
    # 'DepotAppCustomization',
    # 'DepotEntry',
    # 'DepotEntryGroupRel',
    # 'DispatchLog',
    # 'DispatchTrigger',
    # 'DLContent',
    # 'DLFileEntry',
    # 'DLFileEntryMetadata',
    # 'DLFileEntryType',
    # 'DLFileEntryTypes_DLFolders',
    # 'DLFileShortcut',
    # 'DLFileVersion',
    # 'DLFileVersionPreview',
    # 'DLFolder',
    # 'DLOpenerFileEntryReference',
    # 'DLStorageQuota',
    # 'DLSyncEvent',
    # 'EmailAddress',
    # 'ExpandoColumn',
    # 'ExpandoRow',
    # 'ExpandoTable',
    # 'ExpandoValue',
    # 'ExportImportConfiguration',
    # 'FragmentCollection',
    # 'FragmentComposition',
    # 'FragmentEntry',
    # 'FragmentEntryLink',
    # 'FragmentEntryVersion',
    # 'FriendlyURLEntry',
    # 'FriendlyURLEntryLocalization',
    # 'FriendlyURLEntryMapping',
    # 'Group_',
    # 'Groups_Orgs',
    # 'Groups_Roles',
    # 'Groups_UserGroups',
    # 'IM_MemberRequest',
    # 'Image',
    # 'JournalArticle',
    # 'JournalArticleLocalization',
    # 'JournalArticleResource',
    # 'JournalContentSearch',
    # 'JournalFeed',
    # 'JournalFolder',
    # 'JSONStorageEntry',
    # 'KaleoAction',
    # 'KaleoCondition',
    # 'KaleoDefinition',
    # 'KaleoDefinitionVersion',
    # 'KaleoInstance',
    # 'KaleoInstanceToken',
    # 'KaleoLog',
    # 'KaleoNode',
    # 'KaleoNotification',
    # 'KaleoNotificationRecipient',
    # 'KaleoProcess',
    # 'KaleoProcessLink',
    # 'KaleoTask',
    # 'KaleoTaskAssignment',
    # 'KaleoTaskAssignmentInstance',
    # 'KaleoTaskForm',
    # 'KaleoTaskFormInstance',
    # 'KaleoTaskInstanceToken',
    # 'KaleoTimer',
    # 'KaleoTimerInstanceToken',
    # 'KaleoTransition',
    # 'KBArticle',
    # 'KBComment',
    # 'KBFolder',
    # 'KBTemplate',
    # 'Layout',
    # 'LayoutBranch',
    # 'LayoutClassedModelUsage',
    # 'LayoutFriendlyURL',
    # 'LayoutLocalization',
    # 'LayoutPageTemplateCollection',
    # 'LayoutPageTemplateEntry',
    # 'LayoutPageTemplateStructure',
    # 'LayoutPageTemplateStructureRel',
    # 'LayoutPrototype',
    # 'LayoutRevision',
    # 'LayoutSEOEntry',
    # 'LayoutSEOSite',
    # 'LayoutSet',
    # 'LayoutSetBranch',
    # 'LayoutSetPrototype',
    # 'LayoutUtilityPageEntry',
    # 'ListType',
    # 'ListTypeDefinition',
    # 'ListTypeEntry',
    # 'Lock_',
    # 'Marketplace_App',
    # 'Marketplace_Module',
    # 'MBBan',
    # 'MBCategory',
    # 'MBDiscussion',
    # 'MBMailingList',
    # 'MBMessage',
    # 'MBSuspiciousActivity',
    # 'MBThread',
    # 'MBThreadFlag',
    # 'MDRAction',
    # 'MDRRule',
    # 'MDRRuleGroup',
    # 'MDRRuleGroupInstance',
    # 'MembershipRequest',
    # 'MFAEmailOTPEntry',
    # 'MFAFIDO2CredentialEntry',
    # 'MFATimeBasedOTPEntry',
    # 'NotificationQueueEntry',
    # 'NotificationRecipient',
    # 'NotificationRecipientSetting',
    # 'NotificationTemplate',
    # 'NQueueEntryAttachment',
    # 'NTemplateAttachment',
    # 'OA2Auths_OA2ScopeGrants',
    # 'OAuth2Application',
    # 'OAuth2ApplicationScopeAliases',
    # 'OAuth2Authorization',
    # 'OAuth2ScopeGrant',
    # 'OAuthClientASLocalMetadata',
    # 'OAuthClientEntry',
    # 'ObjectAction',
    # 'ObjectDefinition',
    # 'ObjectEntry',
    # 'ObjectField',
    # 'ObjectFieldSetting',
    # 'ObjectFilter',
    # 'ObjectFolder',
    # 'ObjectLayout',
    # 'ObjectLayoutBox',
    # 'ObjectLayoutColumn',
    # 'ObjectLayoutRow',
    # 'ObjectLayoutTab',
    # 'ObjectRelationship',
    # 'ObjectState',
    # 'ObjectStateFlow',
    # 'ObjectStateTransition',
    # 'ObjectValidationRule',
    # 'ObjectValidationRuleSetting',
    # 'ObjectView',
    # 'ObjectViewColumn',
    # 'ObjectViewFilterColumn',
    # 'ObjectViewSortColumn',
    # 'OpenIdConnectSession',
    # 'Organization_',
    # 'Organization_x_40201',
    # 'Organization_x_447240',
    # 'OrgLabor',
    # 'PasswordPolicy',
    # 'PasswordPolicyRel',
    # 'PasswordTracker',
    # 'Phone',
    # 'PLOEntry',
    # 'PluginSetting',
    # 'PortalPreferences',
    # 'PortalPreferenceValue',
    # 'Portlet',
    # 'PortletItem',
    # 'PortletPreferences',
    # 'PortletPreferenceValue',
    # 'PRESERVE_RESIDENT_USER_ID',
    # 'QUARTZ_BLOB_TRIGGERS',
    # 'QUARTZ_CALENDARS',
    # 'QUARTZ_CRON_TRIGGERS',
    # 'QUARTZ_FIRED_TRIGGERS',
    # 'QUARTZ_JOB_DETAILS',
    # 'QUARTZ_LOCKS',
    # 'QUARTZ_PAUSED_TRIGGER_GRPS',
    # 'QUARTZ_SCHEDULER_STATE',
    # 'QUARTZ_SIMPLE_TRIGGERS',
    # 'QUARTZ_SIMPROP_TRIGGERS',
    # 'QUARTZ_TRIGGERS',
    # 'RatingsEntry',
    # 'RatingsStats',
    # 'ReadingTimeEntry',
    # 'RecentLayoutBranch',
    # 'RecentLayoutRevision',
    # 'RecentLayoutSetBranch',
    # 'RedirectEntry',
    # 'RedirectNotFoundEntry',
    # 'Region',
    # 'RegionLocalization',
    # 'Release_',
    # 'Repository',
    # 'RepositoryEntry',
    # 'ResourceAction',
    # 'ResourcePermission',
    # 'Role_',
    # 'SamlIdpSpConnection',
    # 'SamlIdpSpSession',
    # 'SamlIdpSsoSession',
    # 'SamlPeerBinding',
    # 'SamlSpAuthRequest',
    # 'SamlSpIdpConnection',
    # 'SamlSpMessage',
    # 'SamlSpSession',
    # 'SAPEntry',
    # 'SegmentsEntry',
    # 'SegmentsEntryRel',
    # 'SegmentsEntryRole',
    # 'SegmentsExperience',
    # 'SegmentsExperiment',
    # 'SegmentsExperimentRel',
    # 'ServiceComponent',
    # 'SharepointOAuth2TokenEntry',
    # 'SharingEntry',
    # 'SiteFriendlyURL',
    # 'SiteNavigationMenu',
    # 'SiteNavigationMenuItem',
    # 'SocialActivity',
    # 'SocialActivityAchievement',
    # 'SocialActivityCounter',
    # 'SocialActivityLimit',
    # 'SocialActivitySet',
    # 'SocialActivitySetting',
    # 'SocialRelation',
    # 'SocialRequest',
    # 'StyleBookEntry',
    # 'StyleBookEntryVersion',
    # 'Subscription',
    # 'SXPBlueprint',
    # 'SXPElement',
    # 'SystemEvent',
    # 'Team',
    # 'TECHEM_ARCHIVE_ArchiveFile',
    # 'TECHEM_CostReportFormBody',
    # 'TECHEM_CostReportFormHead',
    # 'TECHEM_TechemArea',
    # 'TECHEM_TechemAreaEmployee',
    # 'TECHEM_TechemAreaToRealestate',
    # 'TECHEM_TechemOrgEmployee',
    # 'TECHEM_TechemResident',
    # 'TechemResidentMeterMapping',
    # 'TemplateEntry',
    # 'TempToDelete',
    # 'Ticket',
    # 'TranslationEntry',
    # 'TrashEntry',
    # 'TrashVersion',
    # 'User_',
    # 'User_x_40201',
    # 'User_x_447240',
    # 'UserGroup',
    # 'UserGroupGroupRole',
    # 'UserGroupRole',
    # 'UserGroups_Teams',
    # 'UserIdMapper',
    # 'UserNotificationDelivery',
    # 'UserNotificationEvent',
    # 'Users_Groups',
    # 'Users_Orgs',
    # 'Users_Roles',
    # 'Users_Teams',
    # 'Users_UserGroups',
    # 'UserTracker',
    # 'UserTrackerPath',
    # 'ViewCountEntry',
    # 'VirtualHost',
    # 'WebDAVProps',
    # 'Website',
    # 'WikiNode',
    # 'WikiPage',
    # 'WikiPageResource',
    # 'WMSLADefinition',
    # 'WMSLADefinitionVersion',
    # 'WorkflowDefinitionLink',
    # 'WorkflowInstanceLink'
]

# Function to create a connection',
def create_connection(config):
#    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};UID={config["user"]};PWD={config["password"]};Encrypt=Yes;TrustServerCertificate=Yes;'
# jdbc:sqlserver://portal-int-cl1-prod-sqlserver.database.windows.net:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword
    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};UID={config["user"]};PWD={config["password"]};Encrypt=Yes;TrustServerCertificate=Yes;hostNameInCertificate=*.database.windows.net;loginTimeout=30'
    return pyodbc.connect(conn_str)

# Function to get the create table query
def get_create_table_query(source_cursor, source_schema, table_name, target_schema):
    # Get column definitions
    source_cursor.execute(f"""
        SELECT 
            COLUMN_NAME, DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, 
            COLUMN_DEFAULT, DATETIME_PRECISION
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = N'{source_schema}' AND TABLE_NAME = N'{table_name}'
    """)
    columns = source_cursor.fetchall()

    column_definitions = []
    for column in columns:
        col_def = f"{column.COLUMN_NAME} {column.DATA_TYPE}"
        # Add length for character types and precision for datetime2
        if column.DATA_TYPE in ['varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary']:
            col_def += f"({column.CHARACTER_MAXIMUM_LENGTH})" if column.CHARACTER_MAXIMUM_LENGTH else "(max)"
        elif column.DATA_TYPE == 'datetime2':
            col_def += f"({column.DATETIME_PRECISION})"

        if column.IS_NULLABLE == 'NO':
            col_def += " NOT NULL"
        if column.COLUMN_DEFAULT:
            col_def += f" DEFAULT {column.COLUMN_DEFAULT}"

        column_definitions.append(col_def)


    # Get primary key information
    source_cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = N'{source_schema}' AND TABLE_NAME = N'{table_name}' AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    """)
    primary_keys = [row.COLUMN_NAME for row in source_cursor.fetchall()]
    
    pk_definition = ''
    if primary_keys:
        pk_definition = f", PRIMARY KEY ({', '.join(primary_keys)})"

    # Combine to form CREATE TABLE statement
    create_table_statement = f"CREATE TABLE {target_schema}.{table_name} ({', '.join(column_definitions)}{pk_definition})"
    return create_table_statement



# Function to copy data from source to target
def copy_data(source_conn, target_conn, source_schema, table_name, target_schema):
    print(f"Copying table {table_name} ...", end="", flush=True)
    start_time = perf_counter()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    total_row_count = get_row_count(source_conn, source_schema, table_name)
    print(f" {total_row_count} rows ...", end="")

    page_count = 0
    offset = 0

    while True:

        page_count += 1

        # Fetch data from source table
        source_cursor.execute(f"SELECT * FROM {source_schema}.{table_name} ORDER BY (SELECT 1) OFFSET {offset} ROWS FETCH NEXT {PAGE_SIZE} ROWS ONLY")
        rows = source_cursor.fetchall()

        if not rows:
            break  # Break the loop if there are no more rows to fetch

        row_count = len(rows)
        if row_count == PAGE_SIZE:
            if page_count == 1:
                print(f" paging {int(total_row_count / PAGE_SIZE + 1)} pages each {PAGE_SIZE} rows, page", end="")
            print(f" {page_count}", end="", flush=True)
        else:
            print(f" writing {row_count} rows ...", end="", flush=True)
        
        # Insert data into target table
        placeholders = ', '.join(['?' for _ in rows[0]])
        target_cursor.fast_executemany = True
        target_cursor.executemany(f"INSERT INTO {target_schema}.{table_name} VALUES ({placeholders})", rows)

        target_conn.commit()

        offset += PAGE_SIZE

    duration_sec = perf_counter() - start_time
    rows_per_sec = int(round(total_row_count / duration_sec))
    print(f" - done in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")

def truncate_table(connection, schema_name, table_name):
    print(f"Truncating table {table_name} ...", end="", flush=True)
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
    connection.commit()
    print(" - done")

def get_row_count(connection, schema_name, table_name) -> int:
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM  {schema_name}.{table_name}")
    total_rows = cursor.fetchone()[0]
    return total_rows

def create_table(source_conn, target_conn, source_schema, table_name, target_schema):
    # Create table in target database (including primary key and null constraints)
    source_cursor = source_conn.cursor()
    create_table_query = get_create_table_query(source_cursor, source_schema, table_name, target_schema)
    target_cursor = target_conn.cursor()
    target_cursor.execute(create_table_query)
    target_conn.commit()

def drop_table_if_exists(conn, schema_name, table_name):
    cursor = conn.cursor()

    # Check if the table exists in the given schema
    cursor.execute("""
        SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, (schema_name, table_name))

    if cursor.fetchone():
        # Table exists, drop it
        drop_query = f"DROP TABLE {schema_name}.{table_name}"
        cursor.execute(drop_query)
        conn.commit()
        print(f"Table {schema_name}.{table_name} dropped successfully.")
    else:
        print(f"Table {schema_name}.{table_name} does not exist.")


def copy_indices(source_conn, target_conn, source_schema, table_name, target_schema):
    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # Query to get index information from the source database
    source_cursor.execute("""
    SELECT 
        i.name AS index_name,
        STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS columns,
        i.is_unique
    FROM 
        sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE 
        t.name = ? AND s.name = ? AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
    GROUP BY i.name, i.is_unique, t.name
    """, (table_name, source_schema))

    # Construct and execute CREATE INDEX statements
    for row in source_cursor:
        index_name, columns, is_unique = row
        unique_clause = "UNIQUE" if is_unique else ""
        create_index_query = f"CREATE {unique_clause} INDEX {index_name} ON {target_schema}.{table_name} ({columns})"
        target_cursor.execute(create_index_query)

    print(f"Indices for table {table_name} in schema {target_schema} copied successfully.")


# Main process
source_conn = None
target_conn = None
try:
    # Create connections
    print(f'connecting to source server {source_config["server"]} db {source_config["database"]}... ', end="")
    source_conn = create_connection(source_config)
    print(' - DONE')
    print(f'connecting to target server {target_config["server"]} db {target_config["database"]}... ', end="")
    target_conn = create_connection(target_config)
    print(' - DONE')

    source_schema = source_config['schema']
    target_schema = target_config['schema']

    # Create table in target database (not working!) - tables need to be created before!
    # source_cursor = source_conn.cursor()
    # create_table_query = get_create_table_query(source_cursor, source_schema, table_name, target_schema)
    # target_cursor = target_conn.cursor()
    # target_cursor.execute(create_table_query)
    # target_conn.commit()

    for table_name in table_names:
        if target_config['drop_table']:
            drop_table_if_exists(target_conn, target_schema, table_name)
        if target_config['create_table']:
            create_table(source_conn, target_conn, source_schema, table_name, target_schema)
        if target_config['copy_indices']:
            copy_indices(source_conn, target_conn, source_schema, table_name, target_schema)
        if target_config['truncate_table']:        
            truncate_table(target_conn, target_schema, table_name)

        # Copy data from source to target
        copy_data(source_conn, target_conn, source_schema, table_name, target_schema)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if source_conn:
        source_conn.close()
    if target_conn:
        target_conn.close()
