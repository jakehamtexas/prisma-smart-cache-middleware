import stringify from "safe-stable-stringify";

type Maybe<T> = T | null;
type ValueOrPromise<T> = T | Promise<T>;
type ValueOf<T> = T[keyof T];

export enum ActionType {
  ReadWrite = "rw",
  ReadWriteUnique = "rw-unique",
  Invalidate = "invalidate",
  InvalidateUnique = "invalidate-unique",
  Noop = "noop",
}

export const ActionTypeByName = {
  findFirst: ActionType.ReadWrite,
  findUnique: ActionType.ReadWriteUnique,
  findMany: ActionType.ReadWrite,
  aggregate: ActionType.ReadWrite,
  count: ActionType.ReadWrite,
  findRaw: ActionType.ReadWrite,
  runCommandRaw: ActionType.Noop,
  queryRaw: ActionType.Noop,
  create: ActionType.Invalidate,
  createMany: ActionType.Invalidate,
  update: ActionType.InvalidateUnique,
  updateMany: ActionType.Invalidate,
  upsert: ActionType.InvalidateUnique,
  delete: ActionType.InvalidateUnique,
  deleteMany: ActionType.Invalidate,
  executeRaw: ActionType.Noop,
} as const;

type ActionTypeByName = typeof ActionTypeByName;

export type ActionName = keyof ActionTypeByName;

export type MiddlewareParams<TModelName extends string> = {
  model?: TModelName;
  action: ActionName;
  args: unknown;
  dataPath: string[];
  runInTransaction: boolean;
};

export type Next<TModelName extends string, T> = (
  params: MiddlewareParams<TModelName>
) => Promise<T>;

type PrismaMiddleware<TModelName extends string, T = unknown> = (
  params: MiddlewareParams<TModelName>,
  next: Next<TModelName, T>
) => Promise<T>;

type SelectActionTypeKeys<TType extends ActionType | ActionType[]> = ValueOf<{
  [K in ActionName]: ActionTypeByName[K] extends (
    TType extends unknown[] ? TType[number] : TType
  )
    ? K
    : never;
}>;
export type UniqueActions = SelectActionTypeKeys<
  ActionType.InvalidateUnique | ActionType.ReadWriteUnique
>;

export type IncludeOrExcludeActions =
  | {
      excludeActions?: ActionName[];
    }
  | {
      includeActions?: ActionName[];
    };

export type SyncOpsOptions = {
  synchronouslyWriteToCache?: boolean;
  synchronouslyDeleteFromCache?: boolean;
};

export type Stringify = (value: unknown) => string;
export type BuildUniqueKey<TModelName extends string> = (
  params: MiddlewareParams<TModelName>,
  stringify: Stringify
) => Maybe<string>;

export type CheckShouldInvalidate<TModelName extends string> = (
  params: MiddlewareParams<TModelName>
) => boolean;

export type PrismaCacheModelOptions<
  TDbValue extends object,
  TAnyModelName extends string,
  TModelName extends TAnyModelName
> = {
  uniquenessChecks?: {
    buildUniqueKey: Record<
      SelectActionTypeKeys<ActionType.ReadWriteUnique>,
      BuildUniqueKey<TModelName>
    > &
      Partial<
        Record<
          SelectActionTypeKeys<ActionType.InvalidateUnique>,
          BuildUniqueKey<TModelName>
        >
      >;
    upsertResultWasUpdate?: (
      params: MiddlewareParams<TModelName>,
      value: TDbValue
    ) => boolean;
    relationships?: Partial<
      Record<TAnyModelName, true | CheckShouldInvalidate<TModelName>>
    >;
  };
} & IncludeOrExcludeActions &
  SyncOpsOptions;

export type PrismaCacheModelOptionsConfig<
  TDbValue extends object,
  TAnyModelName extends string
> = {
  [K in TAnyModelName]?: PrismaCacheModelOptions<TDbValue, TAnyModelName, K>;
};

export type BaseKey<
  TModelName extends string,
  TPrefix extends string = never
> = [TPrefix] extends [never] ? TModelName : `${TModelName}__${TPrefix}`;
export type FullyQualifiedKey<
  TModelName extends string,
  TPrefix extends string = never
> = `${BaseKey<TModelName, TPrefix>}__${string}`;

export interface ModelCache<
  TModelName extends string,
  TDbValue extends object = object,
  TPrefix extends string = never
> {
  get(
    key: FullyQualifiedKey<TModelName, TPrefix>
  ): ValueOrPromise<Maybe<TDbValue>>;
  set(
    key: FullyQualifiedKey<TModelName, TPrefix>,
    value: TDbValue
  ): ValueOrPromise<void>;
  delete(key: FullyQualifiedKey<TModelName, TPrefix>): ValueOrPromise<void>;
  clear(): ValueOrPromise<void>;
  clearModelKeys(baseKey?: BaseKey<TModelName, TPrefix>): ValueOrPromise<void>;
}

type LogMethod = (...args: string[]) => void;
export type Logger = { log: LogMethod; error: LogMethod };

export type TreatUnknownActionAs<TModelName extends string> = (
  params: MiddlewareParams<TModelName>
) => Maybe<ActionType>;

export type PrismaCachingMiddlewareConfig<
  TDbValue extends object,
  TModelName extends string,
  TPrefix extends string = never
> = IncludeOrExcludeActions &
  SyncOpsOptions & {
    cache: {
      provider: (
        modelName: TModelName
      ) => ModelCache<TModelName, TDbValue, TPrefix>;
      keyPrefix?: TPrefix;
    };
    loggerProvider?: () => Logger;
    treatUnknownActionAs?: TreatUnknownActionAs<TModelName>;
    modelOptions: PrismaCacheModelOptionsConfig<TDbValue, TModelName>;
  };

function hasUnknownAction(params: MiddlewareParams<string>): boolean {
  return !Object.keys(ActionTypeByName).includes(params.action);
}

function makeIsOfTypeFnBy<TModelName extends string = string>(
  treatUnknownActionAsCb: TreatUnknownActionAs<TModelName> | undefined
): <TType extends ActionType | ActionType[]>(
  type: TType
) => (params: MiddlewareParams<TModelName>) => params is Omit<
  MiddlewareParams<TModelName>,
  "action"
> & {
  action: SelectActionTypeKeys<TType>;
} {
  return <TType extends ActionType | ActionType[]>(type: TType) => {
    const actionNamesOfType = Object.entries(ActionTypeByName)
      .filter(([, actionType]) =>
        Array.isArray(type) ? type.includes(actionType) : actionType === type
      )
      .map(([actionName]) => actionName);
    return (
      params: MiddlewareParams<TModelName>
    ): params is Omit<MiddlewareParams<TModelName>, "action"> & {
      action: SelectActionTypeKeys<TType>;
    } =>
      actionNamesOfType.includes(params.action) ||
      (hasUnknownAction(params) && treatUnknownActionAsCb?.(params) === type);
  };
}

function defaultBuildKey(obj: object, stringify: Stringify): string {
  return stringify(obj);
}

function buildBaseKey<
  TModelName extends string,
  TPrefix extends string = never
>(modelName: TModelName, prefix?: TPrefix): BaseKey<TModelName, TPrefix> {
  const withPrefix = prefix && (`${modelName}__${prefix}` as const);

  const baseKey = withPrefix ?? modelName;
  return baseKey as BaseKey<TModelName, TPrefix>;
}

function buildFullyQualifiedKey<
  TModelName extends string,
  TKey extends string,
  TPrefix extends string
>(
  modelName: TModelName,
  prefix: TPrefix | undefined,
  identifierKeyPart: TKey
): FullyQualifiedKey<TModelName, TPrefix> {
  // TODO: Hash this
  const hashedIdentifier = identifierKeyPart;
  return `${buildBaseKey(modelName, prefix)}__${hashedIdentifier}`;
}

function invoke(
  fn: () => ValueOrPromise<void>,
  synchronous = true
): ValueOrPromise<void> {
  if (synchronous) {
    return fn();
  }
  setImmediate(fn);
}

function isLegalMethodWith(
  middlewareConfig: IncludeOrExcludeActions,
  modelConfig: IncludeOrExcludeActions
) {
  return (actionName: ActionName) => {
    if ("excludeActions" in modelConfig && modelConfig.excludeActions) {
      return !modelConfig.excludeActions.includes(actionName);
    }

    if ("includeActions" in modelConfig && modelConfig.includeActions) {
      return modelConfig.includeActions.includes(actionName);
    }

    if (
      "excludeActions" in middlewareConfig &&
      middlewareConfig.excludeActions
    ) {
      return !middlewareConfig.excludeActions.includes(actionName);
    }

    if (
      "includeActions" in middlewareConfig &&
      middlewareConfig.includeActions
    ) {
      return middlewareConfig.includeActions.includes(actionName);
    }

    return true;
  };
}

function createCacheMutateHelpers<TModelCache extends ModelCache<string>>(
  cache: TModelCache,
  middlewareConfig: SyncOpsOptions,
  modelConfig: SyncOpsOptions
) {
  return {
    cacheSet: createCacheMutateHelper(
      cache,
      middlewareConfig,
      modelConfig,
      "set"
    ),
    cacheDelete: createCacheMutateHelper(
      cache,
      middlewareConfig,
      modelConfig,
      "delete"
    ),
    cacheClear: createCacheMutateHelper(
      cache,
      middlewareConfig,
      modelConfig,
      "clearModelKeys"
    ),
  };
}

type MutateMethodName = "delete" | "set" | "clearModelKeys";

function createCacheMutateHelper<TModelCache extends ModelCache<string>>(
  cache: TModelCache,
  middlewareConfig: SyncOpsOptions,
  modelConfig: SyncOpsOptions,
  methodName: "delete"
): (...args: Parameters<TModelCache["delete"]>) => ValueOrPromise<void>;
function createCacheMutateHelper<TModelCache extends ModelCache<string>>(
  cache: TModelCache,
  middlewareConfig: SyncOpsOptions,
  modelConfig: SyncOpsOptions,
  methodName: "set"
): (...args: Parameters<TModelCache["set"]>) => ValueOrPromise<void>;
function createCacheMutateHelper<TModelCache extends ModelCache<string>>(
  cache: TModelCache,
  middlewareConfig: SyncOpsOptions,
  modelConfig: SyncOpsOptions,
  methodName: "clearModelKeys"
): (...args: Parameters<TModelCache["clearModelKeys"]>) => ValueOrPromise<void>;
function createCacheMutateHelper<TModelCache extends ModelCache<string>>(
  cache: TModelCache,
  middlewareConfig: SyncOpsOptions,
  modelConfig: SyncOpsOptions,
  methodName: MutateMethodName
):
  | ((...args: Parameters<TModelCache["delete"]>) => ValueOrPromise<void>)
  | ((...args: Parameters<TModelCache["set"]>) => ValueOrPromise<void>)
  | ((
      ...args: Parameters<TModelCache["clearModelKeys"]>
    ) => ValueOrPromise<void>) {
  const cb = (...args: Parameters<TModelCache[MutateMethodName]>) =>
    invoke(
      () => cache[methodName](...(args as [never, never])),
      methodName === "delete" || methodName === "clearModelKeys"
        ? modelConfig.synchronouslyDeleteFromCache ??
            middlewareConfig.synchronouslyDeleteFromCache
        : modelConfig.synchronouslyWriteToCache ??
            middlewareConfig.synchronouslyWriteToCache
    );

  return cb;
}

function asCheckShouldInvalidateRecordOrThrow<TModelName extends string>(
  record: Partial<Record<TModelName, unknown>> | undefined
): Record<string, CheckShouldInvalidate<TModelName>> {
  if (record && !isCheckShouldInvalidateRecord(record)) {
    throw new Error("Record is of improper type.");
  }

  return record ?? {};
}

function isCheckShouldInvalidateRecord<TModelName extends string>(
  record: Record<string, unknown>
): record is Record<string, CheckShouldInvalidate<TModelName>> {
  return Object.values(record).every(
    (v) => typeof v === "function" || typeof v === "boolean"
  );
}

export function createPrismaCacheMiddleware<
  TModelName extends string,
  TDbValue extends object,
  TPrefix extends string = never
>({
  modelOptions,
  treatUnknownActionAs,
  loggerProvider,
  ...middlewareConfig
}: PrismaCachingMiddlewareConfig<
  TDbValue,
  TModelName,
  TPrefix
>): PrismaMiddleware<TModelName, TDbValue> {
  const makeIsOfTypeFn = makeIsOfTypeFnBy(treatUnknownActionAs);
  const [
    hasAnyReadWriteAction,
    hasAnyInvalidateAction,
    hasNoopAction,
    hasAnyUniqueAction,
  ] = [
    makeIsOfTypeFn([ActionType.ReadWrite, ActionType.ReadWriteUnique]),
    makeIsOfTypeFn([ActionType.Invalidate, ActionType.InvalidateUnique]),
    makeIsOfTypeFn(ActionType.Noop),
    makeIsOfTypeFn([ActionType.ReadWriteUnique, ActionType.InvalidateUnique]),
  ] as const;

  const userLogger = loggerProvider?.() ?? console;

  const prefix = "!Prisma Smart Cache Middleware!";
  const logger: Logger = {
    log: (...args) => userLogger.log(prefix, ...args),
    error: (...args) => userLogger.error(prefix, ...args),
  };

  // From `modelOptions`, what we have is a map of
  // model names and the models which invalidate them.
  // What we need is a map of invalidating model names
  // and the models they invalidate.
  //
  // The following construction inverts and normalizes
  // `modelOptions` to achieve this result.
  const shouldInvalidateRelatedModels: {
    [K in TModelName]?: Partial<Record<TModelName, CheckShouldInvalidate<K>>>;
  } = (
    Object.entries(modelOptions) as [
      TModelName,
      PrismaCacheModelOptions<TDbValue, TModelName, TModelName>
    ][]
  )
    .map(
      ([modelName, { uniquenessChecks }]) =>
        [modelName, uniquenessChecks?.relationships] as const
    )
    .filter(
      (
        tuple
      ): tuple is [
        TModelName,
        Partial<Record<TModelName, true | CheckShouldInvalidate<TModelName>>>
      ] => !!tuple[1]
    )
    .map(
      ([modelName, relationships]) =>
        [
          modelName,
          Object.entries(
            relationships as Record<
              string,
              true | ((params: MiddlewareParams<TModelName>) => boolean)
            >
          ).map(
            ([relatedModelName, shouldCheckOrFn]) =>
              [
                relatedModelName as TModelName,
                typeof shouldCheckOrFn === "boolean"
                  ? () => shouldCheckOrFn
                  : shouldCheckOrFn,
              ] as const
          ),
        ] as const
    )
    .reduce(
      (acc, [modelName, relatedModelNameCheckFnTuples]) => {
        relatedModelNameCheckFnTuples.forEach(([relatedModelName, checkFn]) => {
          acc[relatedModelName] = {
            ...(acc[relatedModelName] ?? {}),
            [modelName]: checkFn,
          };
        });
        return acc;
      },
      {} as {
        [K in TModelName]?: Partial<
          Record<TModelName, CheckShouldInvalidate<K>>
        >;
      }
    );

  return async (params, next) => {
    const [cache, options] =
      (params.model && [
        middlewareConfig.cache.provider(params.model),
        modelOptions[params.model],
      ]) ??
      [];

    const isUnhandledAction = !Object.values(ActionType)
      .map((actionType) => makeIsOfTypeFn(actionType))
      .some((fn) => fn(params));

    const isLegalMethod =
      options && isLegalMethodWith(middlewareConfig, options);

    if (isUnhandledAction) {
      logger.log(
        `Unrecognized Prisma action: ${params.action}. Middleware not run for this action.\n` +
          "This is potentially unsafe! Use `treatUnknownActionAsCb` to handle it."
      );
    }

    if (
      !params.model ||
      !cache ||
      !options ||
      hasNoopAction(params) ||
      isUnhandledAction ||
      !isLegalMethod?.(params.action)
    ) {
      return next(params);
    }

    const { cacheDelete, cacheSet, cacheClear } = createCacheMutateHelpers(
      cache,
      middlewareConfig,
      options
    );

    const buildUniqueKey =
      hasAnyUniqueAction(params) &&
      options.uniquenessChecks?.buildUniqueKey?.[params.action];

    const paramsKey = defaultBuildKey(params, stringify);
    const uniqueKey = buildUniqueKey && buildUniqueKey(params, stringify);

    const whenBuildUniqueDefinedUniqueKeyCalculationFailed: boolean =
      !uniqueKey && !!buildUniqueKey;

    const fullyQualifiedKey: Maybe<FullyQualifiedKey<TModelName, TPrefix>> =
      whenBuildUniqueDefinedUniqueKeyCalculationFailed
        ? null
        : buildFullyQualifiedKey(
            params.model,
            middlewareConfig.cache.keyPrefix,
            uniqueKey ? `${paramsKey}${uniqueKey}` : paramsKey
          );

    const canReadWriteCache =
      hasAnyReadWriteAction(params) && !!fullyQualifiedKey;
    if (canReadWriteCache) {
      const cached = await cache.get(fullyQualifiedKey);

      if (cached) {
        return cached;
      }
    }

    const rv = await next(params);

    if (canReadWriteCache) {
      await cacheSet(fullyQualifiedKey, rv);
      return rv;
    }

    // After this point, we must be handling a cache invalidation case.
    if (!hasAnyInvalidateAction(params)) {
      logger.error(
        "Panic! This is unreachable code. Aborting middleware and returning query result."
      );

      return rv;
    }

    const whenUpsertWasUpdate =
      params.action !== "upsert" ||
      options.uniquenessChecks?.upsertResultWasUpdate?.(params, rv);

    const clearRelatedModelCacheResults = await Promise.allSettled(
      Object.entries(
        asCheckShouldInvalidateRecordOrThrow(
          shouldInvalidateRelatedModels[params.model]
        )
      )
        .filter((tuple): tuple is [TModelName, never] => tuple[1](params))
        .map(([modelName]) => {
          const relatedCache = middlewareConfig.cache.provider(modelName);

          const relatedBaseKey = buildBaseKey(
            modelName,
            middlewareConfig.cache.keyPrefix
          );
          const { cacheClear: clearRelatedCache } = createCacheMutateHelpers(
            relatedCache,
            middlewareConfig,
            modelOptions[modelName] ?? {}
          );

          return clearRelatedCache(relatedBaseKey);
        }) ?? []
    );

    const rejectedPromises = clearRelatedModelCacheResults.filter(
      (v): v is PromiseRejectedResult => v.status === "rejected"
    );

    if (rejectedPromises.length) {
      logger.error("Related model cache invalidation FAILED.");
      logger.error(...rejectedPromises.map((v) => v.reason));
    }

    if (fullyQualifiedKey && whenUpsertWasUpdate) {
      await cacheDelete(fullyQualifiedKey);
    } else {
      // If a key fails calculation, or the action
      // requires general invalidation (is not Unique),
      // we invalidate the entire model's collection.
      const baseKey = buildBaseKey(
        params.model,
        middlewareConfig.cache.keyPrefix
      );

      await cacheClear(baseKey);
    }

    return rv;
  };
}
