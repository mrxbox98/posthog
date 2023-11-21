import { ToolbarMenu } from '~/toolbar/bar/ToolbarMenu'
import clsx from 'clsx'
import { useActions, useValues } from 'kea'
import { featureFlagsLogic } from '~/toolbar/flags/featureFlagsLogic'
import { toolbarConfigLogic } from '~/toolbar/toolbarConfigLogic'
import { urls } from 'scenes/urls'
import { IconOpenInNew } from 'lib/lemon-ui/icons'
import { LemonSwitch } from 'lib/lemon-ui/LemonSwitch'
import { AnimatedCollapsible } from 'lib/components/AnimatedCollapsible'
import { LemonCheckbox } from 'lib/lemon-ui/LemonCheckbox'
import { Spinner } from 'lib/lemon-ui/Spinner'
import { LemonInput } from 'lib/lemon-ui/LemonInput'
import { Link } from 'lib/lemon-ui/Link'

export const FlagsToolbarMenu = (): JSX.Element => {
    const { searchTerm, filteredFlags, userFlagsLoading } = useValues(featureFlagsLogic)
    const { setSearchTerm, setOverriddenUserFlag, deleteOverriddenUserFlag } = useActions(featureFlagsLogic)
    const { apiURL } = useValues(toolbarConfigLogic)
    return (
        <ToolbarMenu>
            <ToolbarMenu.Header>
                <LemonInput
                    autoFocus
                    placeholder="Search"
                    fullWidth
                    type={'search'}
                    value={searchTerm}
                    onChange={(s) => setSearchTerm(s)}
                />
            </ToolbarMenu.Header>

            <ToolbarMenu.Body>
                <div className="space-y-px">
                    {filteredFlags.length > 0 ? (
                        filteredFlags.map(({ feature_flag, value, hasOverride, hasVariants, currentValue }) => (
                            <div className={clsx('p-1 rounded', hasOverride && 'bg-mark')} key={feature_flag.key}>
                                <div className={clsx('flex flex-row items-center', 'FeatureFlagRow__header')}>
                                    <div className="flex-1 truncate">
                                        <Link
                                            className="font-medium"
                                            to={`${apiURL}${
                                                feature_flag.id
                                                    ? urls.featureFlag(feature_flag.id)
                                                    : urls.featureFlags()
                                            }`}
                                            subtle
                                            target="_blank"
                                        >
                                            {feature_flag.key}
                                            <IconOpenInNew />
                                        </Link>
                                    </div>

                                    <LemonSwitch
                                        checked={!!currentValue}
                                        onChange={(checked) => {
                                            const newValue =
                                                hasVariants && checked
                                                    ? (feature_flag.filters?.multivariate?.variants[0]?.key as string)
                                                    : checked
                                            if (newValue === value && hasOverride) {
                                                deleteOverriddenUserFlag(feature_flag.key)
                                            } else {
                                                setOverriddenUserFlag(feature_flag.key, newValue)
                                            }
                                        }}
                                    />
                                </div>

                                <AnimatedCollapsible collapsed={!hasVariants || !currentValue}>
                                    <div
                                        className={clsx(
                                            'variant-radio-group flex flex-col w-full px-4 py-2 ml-8',
                                            hasOverride && 'overridden'
                                        )}
                                    >
                                        {feature_flag.filters?.multivariate?.variants.map((variant) => (
                                            <LemonCheckbox
                                                key={variant.key}
                                                fullWidth
                                                checked={currentValue === variant.key}
                                                label={`${variant.key} - ${variant.name} (${variant.rollout_percentage}%)`}
                                                onChange={() => {
                                                    const newValue = variant.key
                                                    if (newValue === value && hasOverride) {
                                                        deleteOverriddenUserFlag(feature_flag.key)
                                                    } else {
                                                        setOverriddenUserFlag(feature_flag.key, newValue)
                                                    }
                                                }}
                                                className={clsx(
                                                    currentValue === variant.key &&
                                                        'font-bold rounded bg-primary-highlight',
                                                    'px-2 py-1'
                                                )}
                                            />
                                        ))}
                                    </div>
                                </AnimatedCollapsible>
                            </div>
                        ))
                    ) : (
                        <div className={'FeatureFlagRow flex flex-row items-center px-2 py-1'}>
                            {userFlagsLoading ? (
                                <span className="flex-1 flex justify-center items-center p-4">
                                    <Spinner className="text-2xl" />
                                </span>
                            ) : (
                                `No ${searchTerm.length ? 'matching ' : ''}feature flags found.`
                            )}
                        </div>
                    )}
                </div>
            </ToolbarMenu.Body>

            <ToolbarMenu.Footer>
                <span className="text-xs">Note: overriding feature flags will only affect this browser.</span>
            </ToolbarMenu.Footer>
        </ToolbarMenu>
    )
}
