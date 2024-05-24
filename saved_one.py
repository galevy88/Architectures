def compute(spark, calcDate, source_tables, feature_sets, logger, custom_attributes):
    from pyspark.sql.functions import col, expr, mean, stddev, sum, variance, min, max, last, count, lit, to_date, \
        date_format
    print("calcDate:")
    print(calcDate)
    fs_calc_date_col = to_date(lit(calcDate))

    df = feature_sets["sm_rev_sim_hourly_features_all_users"]

    df = df.withColumn('fs_calc_date', fs_calc_date_col)
    print("DF Before:")
    df.show(1000)

    numeric_features = [
        "transactions_coins", "hourly_gross_rev", "hourly_net_revenue", "hourly_payments",
        "hourly_bonus_count", "hourly_bonus_coins", "highest_bonus_coins", "level_up_bonus_count",
        "spins", "win_coins", "win_contests", "hourly_prize_coins", "highest_prize_coins",
        "total_coins_received", "bet_coins", "net_coins_received", "highest_free_spins_count",
        "highest_win", "highest_bet", "special_mega_bonus_ind", "lucy_lotto_bonus_ind",
        "jp_win_amount", "jp_compensation_win_amount", "jp_win_count", "jp_win_compensation_count",
        "first_piggy_balance", "last_piggy_balance", "collectibles_added_cards_count",
        "collectibles_finish_set_count", "collectibles_removed_cards_count",
        "hourly_sessions_amount", "last_level", "first_level", "balance_start_hour",
        "first_tier_id", "last_tier_id", "balance_end_hour"]

    agg_exprs = [last(col('fs_calc_hour')).alias('last_hour_found'), last(col('fs_calc_hour')).alias('fs_calc_hour'),
                 count(col('fs_calc_hour')).alias('record_count')]
    for c in numeric_features:
        agg_exprs.extend([
            mean(col(c)).alias(c + "_mean"),
            expr(f"percentile_approx({c}, 0.5)").alias(c + "_median"),
            stddev(col(c)).alias(c + "_stddev"),
            sum(col(c)).alias(c + "_sum"),
            variance(col(c)).alias(c + "_variance"),
            min(col(c)).alias(c + "_min"),
            max(col(c)).alias(c + "_max"),
            last(col(c)).alias(c + "_last")])

    aggregated_df = df.groupBy("user_id", "fs_calc_date").agg(*agg_exprs)

    print("aggregated_df AFTER:")
    aggregated_df.show(1000)

    return aggregated_df