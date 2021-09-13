import React from 'react'
import { useResource } from 'rest-hooks'

import { ImplementationTable } from '@app/components/EntityTable'
import { Routes } from '@app/pages/routes'
import useRouter from '@app/hooks/useRouter'
import { Source } from '@app/core/resources/Source'
import ConnectionResource from '@app/core/resources/Connection'
import { getEntityTableData } from '@app/components/EntityTable/utils'
import { EntityTableDataItem } from '@app/components/EntityTable/types'
import SourceDefinitionResource from '@app/core/resources/SourceDefinition'
import useWorkspace from '@app/hooks/services/useWorkspace'

type IProps = {
    sources: Source[]
}

const SourcesTable: React.FC<IProps> = ({ sources }) => {
    const { push } = useRouter()
    const { workspace } = useWorkspace()
    const { connections } = useResource(ConnectionResource.listShape(), {
        workspaceId: workspace.workspaceId,
    })

    const { sourceDefinitions } = useResource(
        SourceDefinitionResource.listShape(),
        {
            workspaceId: workspace.workspaceId,
        }
    )

    const data = getEntityTableData(
        sources,
        connections,
        sourceDefinitions,
        'source'
    )

    const clickRow = (source: EntityTableDataItem) =>
        push(`${Routes.Source}/${source.entityId}`)

    return (
        <ImplementationTable
            data={data}
            onClickRow={clickRow}
            entity="source"
        />
    )
}

export default SourcesTable
