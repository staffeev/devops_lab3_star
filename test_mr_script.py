def test_mapreduce():
    from mr_script import DAUmetric
    dau = DAUmetric(args=["log.tsv"])
    with dau.make_runner() as runner:
        runner.run()
        data = list(dau.parse_output(runner.cat_output()))
        assert data == [
            ("2022-02-04", 24),
            ("2022-02-05", 54),
            ("2022-02-06", 55),
            ("2022-02-07", 68),
            ("2022-02-08", 77),
            ("2022-02-09", 69),
            ("2022-02-10", 85),
            ("2022-02-11", 85),
            ("2022-02-12", 74),
            ("2022-02-13", 85),
            ("2022-02-14", 70),
            ("2022-02-15", 72),
            ("2022-02-16", 80),
            ("2022-02-17", 74),
            ("2022-02-18", 95),
            ("2022-02-19", 77),
            ("2022-02-20", 79),
            ("2022-02-21", 83),
            ("2022-02-22", 76),
            ("2022-02-23", 90),
            ("2022-02-24", 70),
            ("2022-02-25", 77),
            ("2022-02-26", 93),
            ("2022-02-27", 63),
            ("2022-02-28", 76),
        ]
