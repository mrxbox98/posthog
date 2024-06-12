import { LemonButton } from '@posthog/lemon-ui'
import { useValues } from 'kea'
import { FEATURE_FLAGS } from 'lib/constants'
import { featureFlagLogic, FeatureFlagsSet } from 'lib/logic/featureFlagLogic'

import { BillingProductV2AddonType, BillingProductV2Type, BillingV2FeatureType, BillingV2Type } from '~/types'

interface PayGateMiniButtonVariantProps {
    gateVariant: 'add-card' | 'contact-sales' | 'move-to-cloud' | null
    productWithFeature: BillingProductV2AddonType | BillingProductV2Type
    featureInfo: BillingV2FeatureType
    onCtaClick: () => void
    billing: BillingV2Type | null
    isAddonProduct?: boolean
}

export const PayGateMiniButtonVariant = ({
    gateVariant,
    productWithFeature,
    featureInfo,
    onCtaClick,
    billing,
    isAddonProduct,
}: PayGateMiniButtonVariantProps): JSX.Element => {
    const { featureFlags } = useValues(featureFlagLogic)
    return (
        <LemonButton
            to={getCtaLink(
                gateVariant,
                productWithFeature,
                featureInfo,
                featureFlags,
                billing?.subscription_level,
                isAddonProduct
            )}
            disableClientSideRouting={gateVariant === 'add-card' && !isAddonProduct}
            type="primary"
            center
            onClick={onCtaClick}
        >
            {getCtaLabel(gateVariant)}
        </LemonButton>
    )
}

const getCtaLink = (
    gateVariant: 'add-card' | 'contact-sales' | 'move-to-cloud' | null,
    productWithFeature: BillingProductV2AddonType | BillingProductV2Type,
    featureInfo: BillingV2FeatureType,
    featureFlags: FeatureFlagsSet,
    subscriptionLevel?: BillingV2Type['subscription_level'],
    isAddonProduct?: boolean
): string | undefined => {
    if (
        gateVariant === 'add-card' &&
        !isAddonProduct &&
        featureFlags[FEATURE_FLAGS.SUBSCRIBE_TO_ALL_PRODUCTS] &&
        subscriptionLevel === 'free'
    ) {
        return `/api/billing/activate?products=all_products:&redirect_path=/`
    } else if (gateVariant === 'add-card') {
        return `/organization/billing?products=${productWithFeature.type}`
    } else if (gateVariant === 'contact-sales') {
        return `mailto:sales@posthog.com?subject=Inquiring about ${featureInfo.name}`
    } else if (gateVariant === 'move-to-cloud') {
        return 'https://us.posthog.com/signup?utm_medium=in-product&utm_campaign=move-to-cloud'
    }
    return undefined
}

const getCtaLabel = (gateVariant: 'add-card' | 'contact-sales' | 'move-to-cloud' | null): string => {
    if (gateVariant === 'add-card') {
        return 'Upgrade now'
    } else if (gateVariant === 'contact-sales') {
        return 'Contact sales'
    }
    return 'Move to PostHog Cloud'
}
