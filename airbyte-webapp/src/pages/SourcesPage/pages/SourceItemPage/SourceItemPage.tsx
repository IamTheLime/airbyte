import React, { Suspense, useMemo, useState } from 'react'
import { FormattedMessage } from 'react-intl'
import { useResource } from 'rest-hooks'

import { Routes } from '@app/pages/routes'
import { DropDownRow, ImageBlock } from '@app/components'
import PageTitle from '@app/components/PageTitle'
import useRouter from '@app/hooks/useRouter'
import Breadcrumbs from '@app/components/Breadcrumbs'
import {
    ItemTabs,
    StepsTypes,
    TableItemTitle,
} from '@app/components/ConnectorBlocks'
import LoadingPage from '@app/components/LoadingPage'
import MainPageWithScroll from '@app/components/MainPageWithScroll'

import SourceConnectionTable from './components/SourceConnectionTable'
import SourceSettings from './components/SourceSettings'

import ConnectionResource from '@app/core/resources/Connection'
import SourceResource from '@app/core/resources/Source'

import DestinationResource from '@app/core/resources/Destination'
import SourceDefinitionResource from '@app/core/resources/SourceDefinition'
import DestinationsDefinitionResource from '@app/core/resources/DestinationDefinition'
import { getIcon } from 'utils/imageUtils'
import HeadTitle from '@app/components/HeadTitle'
import Placeholder, { ResourceTypes } from '@app/components/Placeholder'
import useWorkspace from '@app/hooks/services/useWorkspace'

const SourceItemPage: React.FC = () => {
    const { query, push } = useRouter<{ id: string }>()
    const { workspace } = useWorkspace()
    const [currentStep, setCurrentStep] = useState<string>(StepsTypes.OVERVIEW)
    const onSelectStep = (id: string) => setCurrentStep(id)

    const { destinations } = useResource(DestinationResource.listShape(), {
        workspaceId: workspace.workspaceId,
    })

    const { destinationDefinitions } = useResource(
        DestinationsDefinitionResource.listShape(),
        {
            workspaceId: workspace.workspaceId,
        }
    )

    const source = useResource(SourceResource.detailShape(), {
        sourceId: query.id,
    })

    const sourceDefinition = useResource(
        SourceDefinitionResource.detailShape(),
        {
            sourceDefinitionId: source.sourceDefinitionId,
        }
    )

    const { connections } = useResource(ConnectionResource.listShape(), {
        workspaceId: workspace.workspaceId,
    })

    const onClickBack = () => push(Routes.Source)

    const breadcrumbsData = [
        {
            name: <FormattedMessage id="sidebar.sources" />,
            onClick: onClickBack,
        },
        { name: source.name },
    ]

    const connectionsWithSource = connections.filter(
        (connectionItem) => connectionItem.sourceId === source.sourceId
    )

    const destinationsDropDownData = useMemo(
        () =>
            destinations.map((item) => {
                const destinationDef = destinationDefinitions.find(
                    (dd) =>
                        dd.destinationDefinitionId ===
                        item.destinationDefinitionId
                )
                return {
                    label: item.name,
                    value: item.destinationId,
                    img: <ImageBlock img={destinationDef?.icon} />,
                }
            }),
        [destinations, destinationDefinitions]
    )

    const onSelect = (data: DropDownRow.IDataItem) => {
        if (data.value === 'create-new-item') {
            push({
                pathname: `${Routes.Source}${Routes.ConnectionNew}`,
                state: { sourceId: source.sourceId },
            })
        } else {
            push({
                pathname: `${Routes.Source}${Routes.ConnectionNew}`,
                state: { destinationId: data.value, sourceId: source.sourceId },
            })
        }
    }

    const renderContent = () => {
        if (currentStep === StepsTypes.SETTINGS) {
            return (
                <SourceSettings
                    currentSource={source}
                    connectionsWithSource={connectionsWithSource}
                />
            )
        }

        return (
            <>
                <TableItemTitle
                    type="destination"
                    dropDownData={destinationsDropDownData}
                    onSelect={onSelect}
                    entity={source.sourceName}
                    entityName={source.name}
                    entityIcon={
                        sourceDefinition ? getIcon(sourceDefinition.icon) : null
                    }
                />
                {connectionsWithSource.length ? (
                    <SourceConnectionTable
                        connections={connectionsWithSource}
                    />
                ) : (
                    <Placeholder resource={ResourceTypes.Destinations} />
                )}
            </>
        )
    }

    return (
        <MainPageWithScroll
            headTitle={
                <HeadTitle
                    titles={[{ id: 'admin.sources' }, { title: source.name }]}
                />
            }
            pageTitle={
                <PageTitle
                    title={<Breadcrumbs data={breadcrumbsData} />}
                    middleComponent={
                        <ItemTabs
                            currentStep={currentStep}
                            setCurrentStep={onSelectStep}
                        />
                    }
                    withLine
                />
            }
        >
            <Suspense fallback={<LoadingPage />}>{renderContent()}</Suspense>
        </MainPageWithScroll>
    )
}

export default SourceItemPage
