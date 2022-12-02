import { useValues } from 'kea'
import { groupsModel } from '~/models/groupsModel'
import { LemonSelect, LemonSelectSection } from '@posthog/lemon-ui'
import { groupsAccessLogic } from 'lib/introductions/groupsAccessLogic'
import { GroupIntroductionFooter } from 'scenes/groups/GroupsIntroduction'

const UNIQUE_USERS = -1

interface AggregationSelectProps {
    aggregationGroupTypeIndex: number | undefined
    onChange: (aggregationGroupTypeIndex: number | undefined) => void
    title?: string
    labelTransform?: (label: string) => string
}

export function AggregationSelect({
    aggregationGroupTypeIndex,
    onChange,
    title = 'Event Aggregation',
    labelTransform = (label) => `Unique ${label}`,
}: AggregationSelectProps): JSX.Element {
    const { groupTypes, aggregationLabel } = useValues(groupsModel)
    const { needsUpgradeForGroups, canStartUsingGroups } = useValues(groupsAccessLogic)

    const optionSections: LemonSelectSection<number>[] = [
        {
            title: title,
            options: [
                {
                    value: UNIQUE_USERS,
                    label: labelTransform('users'),
                },
            ],
        },
    ]

    if (needsUpgradeForGroups || canStartUsingGroups) {
        optionSections[0].footer = <GroupIntroductionFooter needsUpgrade={needsUpgradeForGroups} />
    } else {
        groupTypes.forEach((groupType) => {
            optionSections[0].options.push({
                value: groupType.group_type_index,
                label: labelTransform(`${aggregationLabel(groupType.group_type_index).plural}`),
            })
        })
    }

    return (
        <LemonSelect
            value={aggregationGroupTypeIndex === undefined ? UNIQUE_USERS : aggregationGroupTypeIndex}
            onChange={(value) => {
                if (value !== null) {
                    const groupTypeIndex = value === UNIQUE_USERS ? undefined : value
                    onChange(groupTypeIndex)
                }
            }}
            data-attr="retention-aggregation-selector"
            dropdownMatchSelectWidth={false}
            options={optionSections}
        />
    )
}
