/**
 * MuseAdapter — shape type for the Muse provider adapter.
 *
 * Mirrors the sibling `<X>Adapter` service modules: the driver model
 * ({@link ../Drivers/MuseDriver}) bundles one adapter per instance as a
 * captured closure, so only the shape interface remains as a naming
 * anchor for the driver bundle.
 *
 * @module MuseAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * MuseAdapterShape — per-instance Muse adapter contract. Carries
 * a branded driver kind as the nominal discriminant.
 */
export interface MuseAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
