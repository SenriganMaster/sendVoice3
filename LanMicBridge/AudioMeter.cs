namespace LanMicBridge;

internal sealed class AudioMeter
{
    private const float Epsilon = 1e-9f;
    private const float Alpha = 0.2f;
    private float _smoothedPeakDb;
    private float _smoothedRmsDb;
    private bool _hasValue;

    public float SmoothedPeakDb => _smoothedPeakDb;
    public float SmoothedRmsDb => _smoothedRmsDb;

    public void Update(float peakLinear, float rmsLinear)
    {
        var peakDb = LinearToDb(peakLinear);
        var rmsDb = LinearToDb(rmsLinear);

        if (!_hasValue)
        {
            _smoothedPeakDb = peakDb;
            _smoothedRmsDb = rmsDb;
            _hasValue = true;
            return;
        }

        _smoothedPeakDb = Lerp(_smoothedPeakDb, peakDb, Alpha);
        _smoothedRmsDb = Lerp(_smoothedRmsDb, rmsDb, Alpha);
    }

    public static void ComputePeakRms(ReadOnlySpan<short> pcm, out float peak, out float rms)
    {
        peak = 0f;
        if (pcm.IsEmpty)
        {
            rms = 0f;
            return;
        }

        double sum = 0;
        for (var i = 0; i < pcm.Length; i++)
        {
            var sample = pcm[i];
            var abs = sample == short.MinValue ? 32768 : Math.Abs(sample);
            var value = abs / 32768f;
            if (value > peak)
            {
                peak = value;
            }

            sum += value * value;
        }

        rms = (float)Math.Sqrt(sum / pcm.Length);
    }

    private static float LinearToDb(float value)
    {
        return 20f * (float)Math.Log10(value + Epsilon);
    }

    private static float Lerp(float a, float b, float t)
    {
        return a + (b - a) * t;
    }
}
